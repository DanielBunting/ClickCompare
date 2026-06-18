using CH.Native.BulkInsert;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>
/// A deliberately <b>wide</b> 50M-row workload for stressing insert configs harder than the narrow
/// (id, name, value) table: a random GUID, four Int64s, two Decimals and five DateTime64(3)
/// timestamps — twelve columns of mixed fixed-width and variable-cost serialization.
/// <para>
/// 50M wide rows cannot be materialised (~30+ GB of boxed <c>object[]</c>), so rows are produced by a
/// <b>deterministic, seeded</b> streaming generator: every column is a pure function of the row id
/// (SplitMix64), so generation folds into the measured hot path (realistic for a born-in-app 50M
/// insert) yet stays perfectly reproducible for verification. <see cref="Generate"/> is the single
/// source of truth shared by the streaming generators and the round-trip correctness checks.
/// </para>
/// </summary>
public static class WideWorkload
{
    public const string Table = "wide_target";

    /// <summary>Column order used by every generator and every INSERT — must match the DDL.</summary>
    public static readonly string[] Columns =
    {
        "id", "guid", "l_a", "l_b", "l_c", "d_a", "d_b",
        "ts_created", "ts_updated", "ts_event", "ts_ingested", "ts_expiry",
    };

    /// <summary>
    /// Explicit ClickHouse column types, in <see cref="Columns"/> order. Handed to the Driver's
    /// <c>InsertOptions.ColumnTypes</c> so it can skip the <c>SELECT … WHERE 1=0</c> schema probe
    /// entirely — the strongest form of the "warmup fee" optimisation explored in
    /// ingestion-comparisson.md (stronger than UseSchemaCache, which only caches the probe).
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> ColumnTypes = new Dictionary<string, string>
    {
        ["id"] = "Int64",
        ["guid"] = "UUID",
        ["l_a"] = "Int64",
        ["l_b"] = "Int64",
        ["l_c"] = "Int64",
        ["d_a"] = "Decimal(38, 10)",
        ["d_b"] = "Decimal(18, 4)",
        ["ts_created"] = "DateTime64(3)",
        ["ts_updated"] = "DateTime64(3)",
        ["ts_event"] = "DateTime64(3)",
        ["ts_ingested"] = "DateTime64(3)",
        ["ts_expiry"] = "DateTime64(3)",
    };

    // DateTime64(3) values are scattered after this epoch so they exercise the full sub-second path.
    private static readonly DateTime Epoch = new(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// The canonical wide row for a given id — one deterministic, random-looking sample per column.
    /// Used by the generators (insert path) and the sample round-trip assertions (verification path).
    /// </summary>
    public static WideRow Generate(long id)
    {
        // Independent random streams per column from one seed, so columns don't correlate.
        var k = (ulong)id;
        var rGuidHi = Mix(k ^ 0x1111_1111_1111_1111);
        var rGuidLo = Mix(k ^ 0x2222_2222_2222_2222);
        var rA = Mix(k ^ 0x3333_3333_3333_3333);
        var rB = Mix(k ^ 0x4444_4444_4444_4444);
        var rC = Mix(k ^ 0x5555_5555_5555_5555);
        var rDa = Mix(k ^ 0x6666_6666_6666_6666);
        var rDb = Mix(k ^ 0x7777_7777_7777_7777);

        return new WideRow
        {
            Id = id,
            Guid = GuidFrom(rGuidHi, rGuidLo),
            LA = (long)rA,
            LB = (long)rB,
            LC = (long)rC,
            // Bounded so they fit the declared precision/scale and round-trip exactly.
            // d_a: ≤10 dp, magnitude < 1e4. d_b: ≤4 dp, magnitude < 1e8.
            DA = (decimal)(rDa % 100_000_000_000_000UL) / 10_000_000_000m,
            DB = (decimal)(rDb % 1_000_000_000_000UL) / 10_000m,
            // Five timestamps, each on its own offset stream, truncated to ms (DateTime64(3)).
            TsCreated = TimestampFrom(k, 0xA1, 0, 365),
            TsUpdated = TimestampFrom(k, 0xB2, 365, 730),
            TsEvent = TimestampFrom(k, 0xC3, 0, 30),
            TsIngested = TimestampFrom(k, 0xD4, 730, 760),
            TsExpiry = TimestampFrom(k, 0xE5, 760, 3650),
        };
    }

    /// <summary>
    /// Lazy <c>object[]</c> stream — the Driver's input shape, boxing in the hot path. Yields
    /// <paramref name="rowCount"/> rows starting at <paramref name="startId"/> (for splitting a run
    /// across concurrent streams). Column order matches <see cref="Columns"/>.
    /// </summary>
    public static IEnumerable<object[]> StreamObjects(long rowCount, long startId = 0)
    {
        for (long i = 0; i < rowCount; i++)
            yield return Generate(startId + i).ToObjectArray();
    }

    /// <summary>Lazy typed stream — CH.Native's compiled-extractor input shape, no boxing.</summary>
    public static IEnumerable<WideRow> StreamTyped(long rowCount, long startId = 0)
    {
        for (long i = 0; i < rowCount; i++)
            yield return Generate(startId + i);
    }

    public static async Task ResetTableAsync(DriverConnection conn, CancellationToken ct = default)
    {
        await ExecAsync(conn, $"DROP TABLE IF EXISTS {Table}", ct);
        await ExecAsync(conn, CreateTableSql, ct);
    }

    public static async Task TruncateAsync(DriverConnection conn, CancellationToken ct = default) =>
        await ExecAsync(conn, $"TRUNCATE TABLE IF EXISTS {Table}", ct);

    public static async Task<long> CountAsync(DriverConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM {Table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    /// <summary>Sum of the sequential id column — proves no row was dropped or duplicated.</summary>
    public static async Task<long> SumIdAsync(DriverConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT sum(id) FROM {Table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    /// <summary>0 + 1 + … + (n-1); the expected <see cref="SumIdAsync"/> for a clean insert.</summary>
    public static long ExpectedIdSum(long rowCount) => rowCount * (rowCount - 1) / 2;

    public static readonly string CreateTableSql =
        $"CREATE TABLE {Table} (" +
        "id Int64, guid UUID, l_a Int64, l_b Int64, l_c Int64, " +
        "d_a Decimal(38, 10), d_b Decimal(18, 4), " +
        "ts_created DateTime64(3), ts_updated DateTime64(3), ts_event DateTime64(3), " +
        "ts_ingested DateTime64(3), ts_expiry DateTime64(3)" +
        ") ENGINE = MergeTree ORDER BY id";

    private static async Task ExecAsync(DriverConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TimestampFrom(ulong key, byte salt, int minDays, int maxDays)
    {
        var r = Mix(key ^ (0x0100_0000_0000_0000UL * salt + salt));
        var spanDays = (long)(maxDays - minDays);
        var ms = (long)(r % (ulong)(spanDays * 86_400_000L));
        return Epoch.AddDays(minDays).AddMilliseconds(ms); // already ms-aligned → exact DateTime64(3)
    }

    private static Guid GuidFrom(ulong hi, ulong lo)
    {
        Span<byte> b = stackalloc byte[16];
        BitConverter.TryWriteBytes(b[..8], hi);
        BitConverter.TryWriteBytes(b[8..], lo);
        return new Guid(b);
    }

    // SplitMix64 — cheap, well-distributed, fully deterministic per id.
    private static ulong Mix(ulong z)
    {
        z += 0x9E37_79B9_7F4A_7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EBUL;
        return z ^ (z >> 31);
    }
}

/// <summary>
/// The wide row — a CH.Native typed POCO (compiled-extractor fast path) that also renders to the
/// Driver's <c>object[]</c> shape via <see cref="ToObjectArray"/>. One type feeds both clients so the
/// benchmark compares transport/config, not row models.
/// </summary>
public sealed class WideRow
{
    [Column(Name = "id")] public long Id { get; set; }
    [Column(Name = "guid")] public Guid Guid { get; set; }
    [Column(Name = "l_a")] public long LA { get; set; }
    [Column(Name = "l_b")] public long LB { get; set; }
    [Column(Name = "l_c")] public long LC { get; set; }
    [Column(Name = "d_a")] public decimal DA { get; set; }
    [Column(Name = "d_b")] public decimal DB { get; set; }
    [Column(Name = "ts_created")] public DateTime TsCreated { get; set; }
    [Column(Name = "ts_updated")] public DateTime TsUpdated { get; set; }
    [Column(Name = "ts_event")] public DateTime TsEvent { get; set; }
    [Column(Name = "ts_ingested")] public DateTime TsIngested { get; set; }
    [Column(Name = "ts_expiry")] public DateTime TsExpiry { get; set; }

    // NOTE: keep this type free of extra public *instance* properties — CH.Native's typed
    // BulkInserter<T> maps every gettable instance property to a column, so a stray helper property
    // would be sent as a phantom column the table doesn't have. Render helpers live as methods.

    /// <summary>Driver input shape — boxed values in <see cref="WideWorkload.Columns"/> order.</summary>
    public object[] ToObjectArray() => new object[]
    {
        Id, Guid, LA, LB, LC, DA, DB,
        TsCreated, TsUpdated, TsEvent, TsIngested, TsExpiry,
    };

    /// <summary>Epoch-ms of a timestamp, matching server-side <c>toUnixTimestamp64Milli</c> (tz-agnostic).</summary>
    public static long UnixMs(DateTime utc) =>
        (long)(utc - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
}
