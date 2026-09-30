namespace Messaging;

public sealed record OrderCreated(Guid EventId, Guid OrderId, decimal Amount, string Currency, int Version = 1);
