namespace ClickCompare.Core;

/// <summary>
/// Curated points across MaxDegreeOfParallelism × BatchSize × RowCount.
/// Hand-picked (rather than a full cross product) so each row of the report maps to a claim in
/// ingestion-comparisson.md and meaningless combos (e.g. parallelism over a single batch) are dropped.
/// </summary>
public static class Scenarios
{
    public const int K100 = 100_000;
    public const int M1 = 1_000_000;

    public static readonly IReadOnlyList<InsertScenario> All = new[]
    {
        // --- 1M rows: the headline 9× gap and the multi-stream sweep -----------------------------
        // Driver defaults: 10 × 100K batches, sequential. The ~929 ms staircase baseline.
        new InsertScenario("1M/100K-batch/x1 (driver default)", M1, K100, 1),
        // The multi-stream hypothesis: same batching, more concurrent streams overlapping the
        // per-INSERT fixed cost. Expect a sharp drop from x1 → x4, flattening by x8.
        new InsertScenario("1M/100K-batch/x2", M1, K100, 2),
        new InsertScenario("1M/100K-batch/x4", M1, K100, 4),
        new InsertScenario("1M/100K-batch/x8", M1, K100, 8),
        // Tuned to a single 1M-row batch: architectural penalty gone, serialization on critical path
        // (~320 ms in the doc). Parallelism is a no-op here (one batch) — included as the control.
        new InsertScenario("1M/1M-batch/x1 (one INSERT)", M1, M1, 1),
        new InsertScenario("1M/1M-batch/x4 (one INSERT, parallel no-op)", M1, M1, 4),

        // --- 100K rows: near-parity zone (single batch either way) -------------------------------
        new InsertScenario("100K/100K-batch/x1 (one batch)", K100, K100, 1),
        new InsertScenario("100K/25K-batch/x1 (4 batches, sequential)", K100, 25_000, 1),
        new InsertScenario("100K/25K-batch/x4 (4 batches, parallel)", K100, 25_000, 4),
    };
}
