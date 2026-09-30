using System.Text.Json;
using Confluent.Kafka;
using Messaging;

namespace Billing.Api;

public sealed class OrderConsumer(InvoiceHandler handler, IConfiguration config, ILogger<OrderConsumer> log) : BackgroundService
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
                    var inserted = await handler.Handle(message, stoppingToken);
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
