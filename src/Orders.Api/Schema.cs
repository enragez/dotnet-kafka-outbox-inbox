using Messaging;
using Npgsql;
namespace Orders.Api;

public static class Schema
{
    public static async Task Initialize(NpgsqlDataSource db)
    {
        await using (var connection = await db.OpenConnectionAsync())
            await Database.Execute(connection, null, """
        CREATE TABLE IF NOT EXISTS orders (
            id uuid PRIMARY KEY, amount numeric(18,2) NOT NULL CHECK (amount > 0),
            currency text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS outbox (
            id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, payload text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
        CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
        """, default);

    }
}
