using Confluent.Kafka;
using Messaging;
using Npgsql;

namespace Orders.Api;

public sealed class OutboxPublisher(NpgsqlDataSource db, IConfiguration config, ILogger<OutboxPublisher> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:19092",
            EnableIdempotence = true, Acks = Acks.All, MessageTimeoutMs = 10000
        }).Build();
        var topic = config["Kafka:Topic"] ?? "orders.created.v1";
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = await db.OpenConnectionAsync(stoppingToken);
                await using var tx = await connection.BeginTransactionAsync(stoppingToken);
                // Lock until broker acknowledgement and database commit. A crash between them can duplicate delivery.
                await using var command = new NpgsqlCommand("""
                    SELECT id,aggregate_id,payload FROM outbox WHERE published_at IS NULL
                    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, connection, tx);
                Guid id; string key; string payload;
                await using (var reader = await command.ExecuteReaderAsync(stoppingToken))
                {
                    if (!await reader.ReadAsync(stoppingToken))
                    {
                        await reader.DisposeAsync();
                        await tx.RollbackAsync(stoppingToken);
                        await Task.Delay(500, stoppingToken);
                        continue;
                    }
                    id = reader.GetGuid(0); key = reader.GetGuid(1).ToString(); payload = reader.GetString(2);
                }
                await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = payload }, stoppingToken);
                await Database.Execute(connection, tx, "UPDATE outbox SET published_at=now() WHERE id=$1", stoppingToken, id);
                await tx.CommitAsync(stoppingToken);
                log.LogInformation("Published {EventId}", id);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                log.LogError(ex, "Outbox publish failed; row stays pending");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
