using Messaging;
using Npgsql;
namespace Billing.Api;

public static class Schema
{
    public static async Task Initialize(NpgsqlDataSource db)
    {
        await using (var connection = await db.OpenConnectionAsync())
            await Database.Execute(connection, null, """
        CREATE TABLE IF NOT EXISTS inbox (
            consumer text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(consumer,event_id));
        CREATE TABLE IF NOT EXISTS invoices (
            id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE, amount numeric(18,2) NOT NULL CHECK(amount > 0),
            currency text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
        """, default);

    }
}
