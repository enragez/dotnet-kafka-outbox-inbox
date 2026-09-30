using System.Text.Json;
using Messaging;
using Npgsql;

namespace Orders.Api;

public sealed class OrderStore(NpgsqlDataSource db)
{
    public async Task<OrderCreated> Create(decimal amount, string currency, CancellationToken ct = default)
    {
        if (!OrderValidation.IsValid(amount, currency)) throw new ArgumentException("Invalid order");
        var message = new OrderCreated(Guid.NewGuid(), Guid.NewGuid(), amount, currency);
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await Database.Execute(connection, tx, "INSERT INTO orders(id,amount,currency) VALUES ($1,$2,$3)", ct,
            message.OrderId, amount, currency);
        await Database.Execute(connection, tx, "INSERT INTO outbox(id,aggregate_id,payload) VALUES ($1,$2,$3)", ct,
            message.EventId, message.OrderId, JsonSerializer.Serialize(message));
        await tx.CommitAsync(ct);
        return message;
    }
}
