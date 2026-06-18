namespace ClickCompare.Core;

/// <summary>Which client + serialization path a comparison case exercises.</summary>
public enum Client
{
    /// <summary>clickhouse-cs (ClickHouse.Driver), HTTP, object[] input, batch-per-INSERT.</summary>
    Driver,

    /// <summary>CH.Native, native TCP, strongly-typed compiled extractors, one streamed INSERT.</summary>
    NativeTyped,

    /// <summary>CH.Native, native TCP, object[] input (same shape as the Driver), one streamed INSERT.</summary>
    NativeDynamic,

    /// <summary>CH.Native, native TCP, typed — split into N separate sequential INSERTs (mimics the Driver's fan-out).</summary>
    NativeTypedSplit,

    /// <summary>CH.Native, native TCP, object[] — split into N separate sequential INSERTs (mimics the Driver's fan-out).</summary>
    NativeDynamicSplit,
}

/// <summary>
/// A single point in the native-vs-clickhouse-cs head-to-head. <see cref="BatchSize"/> means
/// "rows per HTTP INSERT" for <see cref="Client.Driver"/> and "internal streaming block size" for
/// the CH.Native clients (which always issue one INSERT). <see cref="Mdop"/> applies to the Driver only.
/// </summary>
public sealed record ComparisonCase(string Name, Client Client, int RowCount, int BatchSize, int Mdop = 1)
{
    public override string ToString() => Name;
}

/// <summary>
/// The head-to-head: CH.Native (latest) vs ClickHouse.Driver (latest) on the same 1M-row workload
/// and the same server. Driver appears at its three operating points (default staircase, tuned
/// single INSERT, best multi-stream) so the native ceiling can be read against each.
/// </summary>
public static class ComparisonCases
{
    public const int M1 = 1_000_000;
    public const int K100 = 100_000;

    public static readonly IReadOnlyList<ComparisonCase> All = new[]
    {
        // clickhouse-cs at its three operating points -------------------------------------------
        new ComparisonCase("Driver default (10x100K seq)", Client.Driver, M1, K100, 1),
        new ComparisonCase("Driver multi-stream (100K x4)", Client.Driver, M1, K100, 4),
        new ComparisonCase("Driver tuned (1x1M INSERT)", Client.Driver, M1, M1, 1),

        // CH.Native — one streamed INSERT, native TCP -------------------------------------------
        new ComparisonCase("CH.Native typed (compiled extractors)", Client.NativeTyped, M1, K100),
        new ComparisonCase("CH.Native dynamic (object[])", Client.NativeDynamic, M1, K100),

        // CH.Native forced into the Driver's shape: 10 × 100K separate sequential INSERTs ---------
        new ComparisonCase("CH.Native typed (10x100K seq)", Client.NativeTypedSplit, M1, K100, 1),
        new ComparisonCase("CH.Native dynamic (10x100K seq)", Client.NativeDynamicSplit, M1, K100, 1),
    };
}
