using Billing.Api;
using Messaging;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Database")
    ?? "Host=localhost;Port=55433;Database=billing;Username=demo;Password=demo"));
builder.Services.AddHostedService<OrderConsumer>();
var app = builder.Build();
await using (var connection = await app.Services.GetRequiredService<NpgsqlDataSource>().OpenConnectionAsync())
    await Database.Execute(connection, null, """
        CREATE TABLE IF NOT EXISTS inbox (
            consumer text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(consumer,event_id));
        CREATE TABLE IF NOT EXISTS invoices (
            id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE, amount numeric(18,2) NOT NULL CHECK(amount > 0),
            currency text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
        """, default);
app.MapGet("/health", () => Results.Ok(new { status = "alive" }));
app.MapGet("/invoices/{orderId:guid}", async (Guid orderId, NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("SELECT id,amount,currency FROM invoices WHERE order_id=$1");
    command.Parameters.AddWithValue(orderId);
    await using var reader = await command.ExecuteReaderAsync(ct);
    return await reader.ReadAsync(ct)
        ? Results.Ok(new { invoiceId = reader.GetGuid(0), orderId, amount = reader.GetDecimal(1), currency = reader.GetString(2) })
        : Results.NotFound();
});
await app.RunAsync();
