using Npgsql;

namespace Messaging;

public static class Database
{
    public static async Task Execute(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
