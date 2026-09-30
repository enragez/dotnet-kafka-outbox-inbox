using Billing.Api;
using Messaging;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Database")
    ?? "Host=localhost;Port=55433;Database=billing;Username=demo;Password=demo"));
builder.Services.AddSingleton<InvoiceHandler>();
builder.Services.AddHostedService<OrderConsumer>();
var app = builder.Build();
await Schema.Initialize(app.Services.GetRequiredService<NpgsqlDataSource>());
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
