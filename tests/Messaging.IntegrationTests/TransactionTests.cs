using System.Text.Json;
using Billing.Api;
using Messaging;
using Npgsql;
using Orders.Api;

namespace Messaging.IntegrationTests;

[TestFixture, Category("Integration"), NonParallelizable]
public sealed class TransactionTests : PostgresFixture
{
    private static OrderCreated Event() => new(Guid.NewGuid(), Guid.NewGuid(), 125.50m, "EUR");

    [Test]
    public async Task Create_commits_order_and_matching_pending_event()
    {
        var message = await new OrderStore(Orders).Create(125.50m, "EUR");
        await using var command = Orders.CreateCommand("""
            SELECT o.amount,o.currency,b.payload,b.published_at
            FROM orders o JOIN outbox b ON b.aggregate_id=o.id WHERE b.id=$1
            """);
        command.Parameters.AddWithValue(message.EventId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetDecimal(0), Is.EqualTo(125.50m));
            Assert.That(reader.GetString(1), Is.EqualTo("EUR"));
            Assert.That(JsonSerializer.Deserialize<OrderCreated>(reader.GetString(2)), Is.EqualTo(message));
            Assert.That(reader.IsDBNull(3), Is.True);
        });
    }

    [Test]
    public async Task Outbox_insert_failure_rolls_back_order()
    {
        // Real DB failure after INSERT orders; rollback is the production transaction's responsibility.
        await Execute(Orders, "ALTER TABLE outbox ADD CONSTRAINT reject_test_event CHECK (false)");
        try
        {
            Assert.ThrowsAsync<PostgresException>(async () => await new OrderStore(Orders).Create(1m, "EUR"));
            Assert.That(await Count(Orders, "orders"), Is.Zero);
            Assert.That(await Count(Orders, "outbox"), Is.Zero);
        }
        finally { await Execute(Orders, "ALTER TABLE outbox DROP CONSTRAINT reject_test_event"); }
    }

    [Test]
    public async Task Duplicate_is_skipped_without_changing_invoice()
    {
        var handler = new InvoiceHandler(Billing);
        var message = Event();
        Assert.That(await handler.Handle(message), Is.True);
        Assert.That(await handler.Handle(message), Is.False);
        Assert.That(await Count(Billing, "inbox"), Is.EqualTo(1));
        Assert.That(await Count(Billing, "invoices"), Is.EqualTo(1));
        await using var command = Billing.CreateCommand("SELECT amount,currency FROM invoices WHERE order_id=$1");
        command.Parameters.AddWithValue(message.OrderId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.That(await reader.ReadAsync(), Is.True);
        Assert.That(reader.GetDecimal(0), Is.EqualTo(message.Amount));
        Assert.That(reader.GetString(1), Is.EqualTo(message.Currency));
    }

    [Test]
    public async Task Concurrent_duplicates_apply_business_effect_once()
    {
        var handler = new InvoiceHandler(Billing);
        var message = Event();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => handler.Handle(message)));
        Assert.That(results.Count(inserted => inserted), Is.EqualTo(1));
        Assert.That(await Count(Billing, "inbox"), Is.EqualTo(1));
        Assert.That(await Count(Billing, "invoices"), Is.EqualTo(1));
    }

    [Test]
    public async Task Invoice_failure_rolls_back_inbox_and_allows_retry()
    {
        var handler = new InvoiceHandler(Billing);
        var message = Event();
        await Execute(Billing, "ALTER TABLE invoices ADD CONSTRAINT reject_test_invoice CHECK (false)");
        try
        {
            Assert.ThrowsAsync<PostgresException>(async () => await handler.Handle(message));
            Assert.That(await Count(Billing, "inbox"), Is.Zero);
            Assert.That(await Count(Billing, "invoices"), Is.Zero);
        }
        finally { await Execute(Billing, "ALTER TABLE invoices DROP CONSTRAINT reject_test_invoice"); }
        Assert.That(await handler.Handle(message), Is.True);
        Assert.That(await Count(Billing, "invoices"), Is.EqualTo(1));
    }

    [Test]
    public async Task Invalid_event_does_not_write_inbox()
    {
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await new InvoiceHandler(Billing).Handle(Event() with { Version = 99 }));
        Assert.That(await Count(Billing, "inbox"), Is.Zero);
        Assert.That(await Count(Billing, "invoices"), Is.Zero);
    }
}
