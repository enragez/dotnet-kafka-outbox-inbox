using System.Text.Json;
using Confluent.Kafka;
using Messaging;
using Npgsql;

namespace Billing.Api;

public sealed class OrderConsumer(NpgsqlDataSource db, IConfiguration config, ILogger<OrderConsumer> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Recreate after failure: never consume and commit past a failed record.
        while (!stoppingToken.IsCancellationRequested)
        {
            using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:19092",
                GroupId = "billing-v1", AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false, EnableAutoOffsetStore = false
            }).Build();
            try
            {
                consumer.Subscribe(config["Kafka:Topic"] ?? "orders.created.v1");
                while (!stoppingToken.IsCancellationRequested)
                {
                    var record = consumer.Consume(stoppingToken);
                    var message = JsonSerializer.Deserialize<OrderCreated>(record.Message.Value)
                        ?? throw new InvalidOperationException("Empty event");
                    if (message.Version != 1 || message.EventId == Guid.Empty || message.OrderId == Guid.Empty
                        || message.Amount <= 0 || decimal.Round(message.Amount, 2) != message.Amount
                        || message.Currency is not { Length: 3 })
                        throw new InvalidOperationException("Unsupported or invalid event");
                    await using var connection = await db.OpenConnectionAsync(stoppingToken);
                    await using var tx = await connection.BeginTransactionAsync(stoppingToken);
                    await using var command = new NpgsqlCommand("""
                        INSERT INTO inbox(consumer,event_id) VALUES ('billing-v1',$1)
                        ON CONFLICT DO NOTHING RETURNING event_id
                        """, connection, tx);
                    command.Parameters.AddWithValue(message.EventId);
                    var inserted = await command.ExecuteScalarAsync(stoppingToken) is not null;
                    if (inserted)
                        await Database.Execute(connection, tx,
                            "INSERT INTO invoices(id,order_id,amount,currency) VALUES ($1,$2,$3,$4)",
                            stoppingToken, Guid.NewGuid(), message.OrderId, message.Amount, message.Currency);
                    await tx.CommitAsync(stoppingToken);
                    // Replay after a failed offset commit is safe: inbox and invoice were committed together.
                    consumer.Commit(record);
                    log.LogInformation("Event {EventId}: {Result}", message.EventId, inserted ? "invoice created" : "duplicate skipped");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { log.LogError(ex, "Processing failed; restart from committed offsets in 2 seconds"); }
            finally { consumer.Close(); }
            await Task.Delay(2000, stoppingToken);
        }
    }
}
