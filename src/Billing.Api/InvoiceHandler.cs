using Messaging;
using Npgsql;

namespace Billing.Api;

public sealed class InvoiceHandler(NpgsqlDataSource db)
{
    public async Task<bool> Handle(OrderCreated message, CancellationToken ct = default)
    {
        OrderValidation.Validate(message);
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand("""
            INSERT INTO inbox(consumer,event_id) VALUES ('billing-v1',$1)
            ON CONFLICT DO NOTHING RETURNING event_id
            """, connection, tx);
        command.Parameters.AddWithValue(message.EventId);
        var inserted = await command.ExecuteScalarAsync(ct) is not null;
        if (inserted)
            await Database.Execute(connection, tx,
                "INSERT INTO invoices(id,order_id,amount,currency) VALUES ($1,$2,$3,$4)",
                ct, Guid.NewGuid(), message.OrderId, message.Amount, message.Currency);
        await tx.CommitAsync(ct);
        return inserted;
    }
}
