using Messaging;
using System.Globalization;

namespace Messaging.UnitTests;

[TestFixture]
[Category("Unit")]
public sealed class OrderValidationTests
{
    [TestCase("0.01", "EUR", true)]
    [TestCase("125.50", "USD", true)]
    [TestCase("9999999999999999.99", "RUB", true)]
    [TestCase("10000000000000000", "EUR", false)]
    [TestCase("0", "EUR", false)]
    [TestCase("-1", "EUR", false)]
    [TestCase("1.001", "EUR", false)]
    [TestCase("1", null, false)]
    [TestCase("1", "", false)]
    [TestCase("1", "EU", false)]
    [TestCase("1", "EURO", false)]
    [TestCase("1", "eur", false)]
    [TestCase("1", "ЕUR", false)] // Cyrillic Е
    [TestCase("1", "E1R", false)]
    [TestCase("1", "E R", false)]
    public void Validates_amount_and_currency(string amount, string? currency, bool expected)
    {
        Assert.That(OrderValidation.IsValid(decimal.Parse(amount, CultureInfo.InvariantCulture), currency), Is.EqualTo(expected));
    }

    private static OrderCreated Valid() => new(Guid.NewGuid(), Guid.NewGuid(), 1m, "EUR");

    [Test]
    public void Accepts_supported_event() => Assert.DoesNotThrow(() => OrderValidation.Validate(Valid()));

    [Test]
    public void Rejects_empty_event_id() =>
        Assert.Throws<InvalidOperationException>(() => OrderValidation.Validate(Valid() with { EventId = Guid.Empty }));

    [Test]
    public void Rejects_empty_order_id() =>
        Assert.Throws<InvalidOperationException>(() => OrderValidation.Validate(Valid() with { OrderId = Guid.Empty }));

    [TestCase(0)]
    [TestCase(2)]
    public void Rejects_unsupported_version(int version) =>
        Assert.Throws<InvalidOperationException>(() => OrderValidation.Validate(Valid() with { Version = version }));

    [Test]
    public void Rejects_amount_outside_database_precision() =>
        Assert.Throws<InvalidOperationException>(() => OrderValidation.Validate(Valid() with { Amount = decimal.MaxValue }));

    [Test]
    public void Rejects_invalid_event_currency() =>
        Assert.Throws<InvalidOperationException>(() => OrderValidation.Validate(Valid() with { Currency = "eur" }));
}
