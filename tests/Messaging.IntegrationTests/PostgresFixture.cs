using Npgsql;
using Testcontainers.PostgreSql;

namespace Messaging.IntegrationTests;

// Separate containers per fixture; no access to the demo Compose databases.
public abstract class PostgresFixture
{
    private PostgreSqlContainer? container;
    protected NpgsqlDataSource Orders = null!;
    protected NpgsqlDataSource Billing = null!;

    [OneTimeSetUp]
    public async Task StartPostgres()
    {
        container = new PostgreSqlBuilder("postgres:17").Build();
        using var startup = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        await container.StartAsync(startup.Token);
        await using var admin = NpgsqlDataSource.Create(container.GetConnectionString());
        await using (var command = admin.CreateCommand("CREATE DATABASE orders"))
            await command.ExecuteNonQueryAsync();
        await using (var command = admin.CreateCommand("CREATE DATABASE billing"))
            await command.ExecuteNonQueryAsync();
        Orders = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Database = "orders" }.ConnectionString);
        Billing = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Database = "billing" }.ConnectionString);
        await global::Orders.Api.Schema.Initialize(Orders);
        await global::Billing.Api.Schema.Initialize(Billing);
    }

    [SetUp]
    public async Task ClearData()
    {
        await Execute(Orders, "TRUNCATE orders,outbox");
        await Execute(Billing, "TRUNCATE inbox,invoices");
    }

    protected static async Task Execute(NpgsqlDataSource db, string sql)
    {
        await using var command = db.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    protected static async Task<long> Count(NpgsqlDataSource db, string table)
    {
        await using var command = db.CreateCommand($"SELECT count(*) FROM {table}");
        return (long)(await command.ExecuteScalarAsync())!;
    }

    [OneTimeTearDown]
    public async Task StopPostgres()
    {
        if (Orders is not null) await Orders.DisposeAsync();
        if (Billing is not null) await Billing.DisposeAsync();
        if (container is not null) await container.DisposeAsync();
    }
}
