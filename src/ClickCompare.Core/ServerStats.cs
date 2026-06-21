using ClickHouse.Driver.ADO;

namespace ClickCompare.Core;

/// <summary>Server-side accounting for one INSERT, read back from <c>system.query_log</c>.</summary>
public readonly record struct ServerStats(
    double QueryDurationMs,    // server-side wall time of the INSERT query
    double CpuMs,              // ProfileEvents['OSCPUVirtualTimeMicroseconds'] — query-thread CPU consumed
    double NetworkRecvMs,      // ProfileEvents['NetworkReceiveElapsedMicroseconds'] — time blocked reading client bytes
    long ReadRows,
    long WrittenRows,
    long WrittenBytes,         // compressed bytes written to parts
    long PeakMemoryBytes);

/// <summary>
/// Pulls the server's own view of an insert from <c>system.query_log</c> — CPU time, time spent blocked
/// on the client socket, bytes written — so a Parquet single-file insert and a native streaming insert
/// can be compared by <i>server work</i>, not client wall-clock. Assumes serial execution (the harness
/// runs one insert at a time), so "the most recent finished INSERT" is unambiguous.
/// </summary>
public static class ServerQueryLog
{
    public static async Task<ServerStats> LatestInsertAsync(
        ClickHouseConnection conn, string table = Workload.Table, CancellationToken ct = default)
    {
        // query_log is written asynchronously; force it so the row we just produced is visible.
        await using (var flush = conn.CreateCommand())
        {
            flush.CommandText = "SYSTEM FLUSH LOGS";
            await flush.ExecuteNonQueryAsync(ct);
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
SELECT query_duration_ms,
       toFloat64(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms,
       toFloat64(ProfileEvents['NetworkReceiveElapsedMicroseconds']) / 1000 AS net_ms,
       read_rows, written_rows, written_bytes, memory_usage
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert'
  AND arrayExists(t -> t LIKE '%{table}', tables)
ORDER BY event_time_microseconds DESC
LIMIT 1";

        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct))
            throw new InvalidOperationException($"No finished INSERT into {table} found in query_log.");

        return new ServerStats(
            Convert.ToDouble(r.GetValue(0)),
            Convert.ToDouble(r.GetValue(1)),
            Convert.ToDouble(r.GetValue(2)),
            Convert.ToInt64(r.GetValue(3)),
            Convert.ToInt64(r.GetValue(4)),
            Convert.ToInt64(r.GetValue(5)),
            Convert.ToInt64(r.GetValue(6)));
    }
}
