using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;

namespace ClickCompare.Core;

/// <summary>
/// One ClickHouse.Driver insert configuration. Every field is a single tuning axis surfaced by the
/// decompiled driver (see the catalog in ingestion-comparisson.md / the investigation): the config
/// list below holds a <b>base</b> plus one-variable-at-a-time variants so each benchmark row isolates
/// exactly one knob.
/// <para>
/// Reachable only via the modern <c>ClickHouseClient.InsertBinaryAsync</c> API — the obsolete
/// <c>ClickHouseBulkCopy</c> can set only BatchSize/Mdop/Format and cannot touch
/// <see cref="UseSchemaCache"/>, <see cref="UseColumnTypes"/>, or <see cref="CustomSettings"/>.
/// </para>
/// </summary>
public sealed record DriverInsertConfig(
    string Name,
    int BatchSize = 100_000,
    int Mdop = 4,
    bool UseSchemaCache = false,
    bool UseColumnTypes = false,
    RowBinaryFormat Format = RowBinaryFormat.RowBinary,
    IReadOnlyDictionary<string, object>? CustomSettings = null,
    int Streams = 1)
{
    /// <summary>Insert-body gzip is hardcoded in the driver (CompressionLevel.Fastest) and is NOT a
    /// tunable insert knob in 1.2.0 — deliberately absent here so no variant pretends to toggle it.</summary>
    public override string ToString() => Name;
}

/// <summary>
/// Base config + single-variable variants for the wide 50M insert. BASE is a sensible, fair default
/// (100K batch, 4-way client parallelism, RowBinary, no schema cache/column types/server settings,
/// one client). Each variant changes exactly one axis so the benchmark attributes the delta cleanly.
/// </summary>
public static class DriverInsertConfigs
{
    public static readonly DriverInsertConfig Base = new("driver: BASE (100k, mdop=4)");

    public static readonly IReadOnlyList<DriverInsertConfig> All = new[]
    {
        Base,

        // --- BatchSize (rows per HTTP INSERT) ---------------------------------------------------
        Base with { Name = "driver: batch=10k", BatchSize = 10_000 },
        Base with { Name = "driver: batch=500k", BatchSize = 500_000 },
        Base with { Name = "driver: batch=1M (one INSERT/stream)", BatchSize = 1_000_000 },

        // --- MaxDegreeOfParallelism (concurrent HTTP INSERTs within one client) -----------------
        Base with { Name = "driver: mdop=1 (sequential)", Mdop = 1 },
        Base with { Name = "driver: mdop=8", Mdop = 8 },
        Base with { Name = "driver: mdop=10", Mdop = 10 },
        Base with { Name = "driver: mdop=16", Mdop = 16 },

        // --- Schema-probe handling (the per-insert "warmup fee") --------------------------------
        Base with { Name = "driver: schema-cache", UseSchemaCache = true },
        Base with { Name = "driver: column-types (skip probe)", UseColumnTypes = true },

        // --- Wire format ------------------------------------------------------------------------
        Base with { Name = "driver: RowBinaryWithDefaults", Format = RowBinaryFormat.RowBinaryWithDefaults },

        // --- Server-side settings (sent as URL query params via CustomSettings) -----------------
        Base with { Name = "driver: async_insert=1", CustomSettings = Settings(("async_insert", "1")) },
        Base with { Name = "driver: async_insert, no-wait",
            CustomSettings = Settings(("async_insert", "1"), ("wait_for_async_insert", "0")) },
        Base with { Name = "driver: max_insert_threads=8", CustomSettings = Settings(("max_insert_threads", "8")) },
        Base with { Name = "driver: parallel_parsing=0",
            CustomSettings = Settings(("input_format_parallel_parsing", "0")) },

        // --- App-level "mass parallelism": N independent clients, disjoint id ranges ------------
        Base with { Name = "driver: streams=2", Streams = 2 },
        Base with { Name = "driver: streams=4", Streams = 4 },
        Base with { Name = "driver: streams=8", Streams = 8 },
        Base with { Name = "driver: streams=10", Streams = 10 },
        // Apples-to-apples with Native at parallelism 10, but with a big batch so 10 streams don't
        // explode into hundreds of small parts (each stream → ~1 INSERT/part instead of ~10).
        Base with { Name = "driver: streams=10, batch=1M", Streams = 10, BatchSize = 1_000_000 },
    };

    private static Dictionary<string, object> Settings(params (string Key, string Value)[] kv)
    {
        var d = new Dictionary<string, object>(kv.Length);
        foreach (var (k, v) in kv) d[k] = v;
        return d;
    }
}

/// <summary>
/// Executes a <see cref="DriverInsertConfig"/> against ClickHouse.Driver via the modern
/// <c>ClickHouseClient</c> API, streaming the wide rows so 50M never materialises. Returns the total
/// rows the driver reports written (summed across streams) for the correctness assertions.
/// </summary>
public static class DriverWideInsertRunner
{
    // The driver's default HttpClient.Timeout is 120 s — far too short for a 50M-row insert.
    private static readonly TimeSpan InsertTimeout = TimeSpan.FromMinutes(30);

    public static async Task<long> RunAsync(
        string connectionString, DriverInsertConfig cfg, long rowCount, CancellationToken ct = default)
    {
        var connStr = new ClickHouseConnectionStringBuilder(connectionString) { Timeout = InsertTimeout }.ToString();

        if (cfg.Streams <= 1)
        {
            using var client = new ClickHouseClient(connStr);
            return await InsertSliceAsync(client, cfg, rowCount, startId: 0, ct);
        }

        // Mass parallelism: one ClickHouseClient per stream (each gets its own connection pool, per
        // the decompiled DefaultPoolHttpClientFactory), each inserting a disjoint contiguous id range.
        var perStream = rowCount / cfg.Streams;
        var clients = new List<ClickHouseClient>(cfg.Streams);
        try
        {
            var tasks = new List<Task<long>>(cfg.Streams);
            for (var s = 0; s < cfg.Streams; s++)
            {
                var startId = (long)s * perStream;
                var count = s == cfg.Streams - 1 ? rowCount - startId : perStream; // last stream takes the remainder
                var client = new ClickHouseClient(connStr);
                clients.Add(client);
                tasks.Add(InsertSliceAsync(client, cfg, count, startId, ct));
            }
            var written = await Task.WhenAll(tasks);
            return written.Sum();
        }
        finally
        {
            foreach (var c in clients) c.Dispose();
        }
    }

    private static Task<long> InsertSliceAsync(
        ClickHouseClient client, DriverInsertConfig cfg, long count, long startId, CancellationToken ct)
    {
        var options = new InsertOptions
        {
            BatchSize = cfg.BatchSize,
            MaxDegreeOfParallelism = cfg.Mdop,
            Format = cfg.Format,
            UseSchemaCache = cfg.UseSchemaCache,
            ColumnTypes = cfg.UseColumnTypes ? WideWorkload.ColumnTypes : null,
            // Copy so concurrent streams never share a mutable dictionary instance.
            CustomSettings = cfg.CustomSettings is null ? null : new Dictionary<string, object>(cfg.CustomSettings),
        };
        return client.InsertBinaryAsync(
            WideWorkload.Table, WideWorkload.Columns, WideWorkload.StreamObjects(count, startId), options, ct);
    }
}
