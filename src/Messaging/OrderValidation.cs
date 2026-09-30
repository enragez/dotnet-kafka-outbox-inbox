namespace Messaging;

public static class OrderValidation
{
    public static bool IsValid(decimal amount, string? currency) =>
        amount > 0 && amount <= 9999999999999999.99m && decimal.Round(amount, 2) == amount
        && currency is { Length: 3 } && currency.All(c => c is >= 'A' and <= 'Z');

    public static void Validate(OrderCreated message)
    {
        if (message.Version != 1 || message.EventId == Guid.Empty || message.OrderId == Guid.Empty
            || !IsValid(message.Amount, message.Currency))
        {
            throw new InvalidOperationException("Unsupported or invalid event");
        }
    }
}