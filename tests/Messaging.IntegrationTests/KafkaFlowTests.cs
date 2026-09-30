using System.Text.Json;
using Billing.Api;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orders.Api;
using Testcontainers.Kafka;

namespace Messaging.IntegrationTests;

[TestFixture, Category("Integration"), NonParallelizable]
public sealed class KafkaFlowTests : PostgresFixture
{
    private KafkaContainer? kafka;

    [OneTimeSetUp]
    public async Task StartKafka()
    {
        kafka = new KafkaBuilder("confluentinc/cp-kafka:7.8.0").Build();
        using var startup = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        await kafka.StartAsync(startup.Token);
    }

    [OneTimeTearDown]
    public async Task StopKafka()
    {
        if (kafka is not null) await kafka.DisposeAsync();
    }

    private async Task<(string Topic, IConfiguration Config)> NewTopic()
    {
        var topic = "test-" + Guid.NewGuid().ToString("N");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka!.GetBootstrapAddress() }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        return (topic, new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = kafka.GetBootstrapAddress(), ["Kafka:Topic"] = topic
        }).Build());
    }

    private long CommittedOffset(string topic)
    {
        using var observer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = kafka!.GetBootstrapAddress(), GroupId = "billing-v1", EnableAutoCommit = false
        }).Build();
        return observer.Committed([new TopicPartition(topic, 0)], TimeSpan.FromSeconds(5))[0].Offset.Value;
    }

    private static async Task Eventually(Func<Task<bool>> condition, string reason)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        while (!await condition())
        {
            if (timeout.IsCancellationRequested) Assert.Fail(reason);
            await Task.Delay(200);
        }
    }

    [Test]
    public async Task Outbox_to_kafka_to_inbox_and_replay_creates_one_invoice()
    {
        var (topic, config) = await NewTopic();
        using var publisher = new OutboxPublisher(Orders, config, NullLogger<OutboxPublisher>.Instance);
        using var consumer = new OrderConsumer(new InvoiceHandler(Billing), config, NullLogger<OrderConsumer>.Instance);
        await consumer.StartAsync(default);
        await publisher.StartAsync(default);
        try
        {
            var message = await new OrderStore(Orders).Create(125.50m, "EUR");
            await Eventually(async () => await Count(Billing, "invoices") == 1 && CommittedOffset(topic) == 1,
                "First event was not processed and committed");
            await Eventually(async () => await Count(Orders, "outbox WHERE published_at IS NOT NULL") == 1,
                "Publisher did not mark the event as published");
            await Execute(Orders, "UPDATE outbox SET published_at=NULL");
            // Offset 2 proves the duplicate was processed, not merely that the first invoice exists.
            await Eventually(() => Task.FromResult(CommittedOffset(topic) == 2), "Duplicate offset was not committed");
            Assert.That(await Count(Billing, "invoices"), Is.EqualTo(1));
            Assert.That(await Count(Billing, "inbox"), Is.EqualTo(1));
            await using var command = Billing.CreateCommand("SELECT order_id,amount,currency FROM invoices");
            await using var reader = await command.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.True);
            Assert.That(reader.GetGuid(0), Is.EqualTo(message.OrderId));
            Assert.That(reader.GetDecimal(1), Is.EqualTo(message.Amount));
            Assert.That(reader.GetString(2), Is.EqualTo(message.Currency));
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await publisher.StopAsync(timeout.Token);
            await consumer.StopAsync(timeout.Token);
        }
    }

    [Test]
    public async Task Database_failure_does_not_commit_offset_and_recovers_after_retry()
    {
        var (topic, config) = await NewTopic();
        var logger = new ErrorSignalLogger();
        using var consumer = new OrderConsumer(new InvoiceHandler(Billing), config, logger);
        await Execute(Billing, "ALTER TABLE invoices ADD CONSTRAINT reject_kafka_invoice CHECK(false)");
        await consumer.StartAsync(default);
        try
        {
            using var producer = new ProducerBuilder<string, string>(new ProducerConfig
            {
                BootstrapServers = kafka!.GetBootstrapAddress(), Acks = Acks.All
            }).Build();
            var message = new OrderCreated(Guid.NewGuid(), Guid.NewGuid(), 42m, "USD");
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = message.OrderId.ToString(), Value = JsonSerializer.Serialize(message)
            });
            var error = await logger.Error.Task.WaitAsync(TimeSpan.FromSeconds(60));
            Assert.That(error, Is.TypeOf<Npgsql.PostgresException>(), "Expected actual invoice insert failure");
            Assert.That(CommittedOffset(topic), Is.LessThanOrEqualTo(0));
            Assert.That(await Count(Billing, "inbox"), Is.Zero);
            Assert.That(await Count(Billing, "invoices"), Is.Zero);
            await Execute(Billing, "ALTER TABLE invoices DROP CONSTRAINT reject_kafka_invoice");
            await Eventually(async () => await Count(Billing, "invoices") == 1 && CommittedOffset(topic) == 1,
                "Consumer failed to retry the uncommitted event");
            Assert.That(await Count(Billing, "inbox"), Is.EqualTo(1));
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await consumer.StopAsync(timeout.Token);
            await Execute(Billing, "ALTER TABLE invoices DROP CONSTRAINT IF EXISTS reject_kafka_invoice");
        }
    }

    private sealed class ErrorSignalLogger : ILogger<OrderConsumer>
    {
        public TaskCompletionSource<Exception> Error { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error && exception is not null) Error.TrySetResult(exception);
        }
    }
}
