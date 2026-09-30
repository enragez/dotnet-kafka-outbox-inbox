using System.Text.Json;
using Messaging;
using Npgsql;
using Orders.Api;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Database")
    ?? "Host=localhost;Port=55432;Database=orders;Username=demo;Password=demo"));
builder.Services.AddHostedService<OutboxPublisher>();
var app = builder.Build();
await using (var connection = await app.Services.GetRequiredService<NpgsqlDataSource>().OpenConnectionAsync())
    await Database.Execute(connection, null, """
        CREATE TABLE IF NOT EXISTS orders (
            id uuid PRIMARY KEY, amount numeric(18,2) NOT NULL CHECK (amount > 0),
            currency text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS outbox (
            id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, payload text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
        CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
        """, default);
app.MapGet("/health", () => Results.Ok(new { status = "alive" }));
app.MapPost("/orders", async (CreateOrder request, NpgsqlDataSource db, CancellationToken ct) =>
{
    if (request.Amount <= 0 || request.Amount > 9999999999999999.99m || decimal.Round(request.Amount, 2) != request.Amount
        || request.Currency is not { Length: 3 } || !request.Currency.All(c => c is >= 'A' and <= 'Z'))
        return Results.BadRequest(new { error = "Positive amount with <= 2 decimals and uppercase 3-letter currency required." });
    var orderId = Guid.NewGuid();
    var message = new OrderCreated(Guid.NewGuid(), orderId, request.Amount, request.Currency);
    await using var connection = await db.OpenConnectionAsync(ct);
    await using var tx = await connection.BeginTransactionAsync(ct);
    await Database.Execute(connection, tx, "INSERT INTO orders(id,amount,currency) VALUES ($1,$2,$3)", ct,
        orderId, request.Amount, request.Currency);
    await Database.Execute(connection, tx, "INSERT INTO outbox(id,aggregate_id,payload) VALUES ($1,$2,$3)", ct,
        message.EventId, orderId, JsonSerializer.Serialize(message));
    await tx.CommitAsync(ct);
    return Results.Created($"/orders/{orderId}", new { orderId, eventId = message.EventId });
});
app.MapGet("/orders/{id:guid}", async (Guid id, NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("SELECT amount,currency FROM orders WHERE id=$1");
    command.Parameters.AddWithValue(id);
    await using var reader = await command.ExecuteReaderAsync(ct);
    return await reader.ReadAsync(ct)
        ? Results.Ok(new { orderId = id, amount = reader.GetDecimal(0), currency = reader.GetString(1) })
        : Results.NotFound();
});
await app.RunAsync();
record CreateOrder(decimal Amount, string Currency);
