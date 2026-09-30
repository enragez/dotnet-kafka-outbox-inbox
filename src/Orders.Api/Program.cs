using Messaging;
using Npgsql;
using Orders.Api;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Database")
    ?? "Host=localhost;Port=55432;Database=orders;Username=demo;Password=demo"));
builder.Services.AddSingleton<OrderStore>();
builder.Services.AddHostedService<OutboxPublisher>();
var app = builder.Build();
await Schema.Initialize(app.Services.GetRequiredService<NpgsqlDataSource>());
app.MapGet("/health", () => Results.Ok(new { status = "alive" }));
app.MapPost("/orders", async (CreateOrder request, OrderStore store, CancellationToken ct) =>
{
    if (!OrderValidation.IsValid(request.Amount, request.Currency))
        return Results.BadRequest(new { error = "Positive amount with <= 2 decimals and uppercase 3-letter currency required." });
    var message = await store.Create(request.Amount, request.Currency, ct);
    var orderId = message.OrderId;
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
