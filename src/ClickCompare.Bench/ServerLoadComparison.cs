using System.Diagnostics;
using ClickCompare.Core;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Bench;

/// <summary>
/// Compares insert paths by <b>server work</b>, not client wall-clock: for each path it runs a single
/// fixed-size INSERT and reads that query back from <c>system.query_log</c> (server duration, query-thread
/// CPU, time blocked on the client socket, bytes written). Answers "does the ClickHouse server actually
/// compute less for a streamed native insert than for a single-file Parquet/RowBinary insert?".
/// <para>
/// Routes: CH.Native streaming (native blocks) · HTTP RowBinary · HTTP Parquet · server-local Parquet
/// <c>file()</c>. RowBinary is the doc's fully-attributed reference — Parquet decode runs partly on
/// background parser threads the query-thread CPU counter misses, so its CPU is under-counted (flagged).
/// </para>
/// Invoke with <c>dotnet run -c Release --project src/ClickCompare.Bench -- server-load</c>
/// (row count from <c>SERVER_ROWS</c>, default 10M).
/// </summary>
public static class ServerLoadComparison
{
    public static async Task RunAsync(int rows, int warmup, int reps)
    {
        await ClickHouseFixture.StartAsync();

        await using var driver = new DriverConnection(ClickHouseFixture.ConnectionString);
        await driver.OpenAsync();
        await using var native = new NativeConnection(ClickHouseFixture.NativeConnectionString);
        await native.OpenAsync(default);
        // Same native protocol, LZ4 wire compression off — isolates "does the server's decompression of
        // native blocks account for its extra CPU vs an uncompressed HTTP body?".
        var noCompConn = ClickHouseFixture.NativeConnectionString.Replace("Compress=true", "Compress=false");
        await using var nativeNoComp = new NativeConnection(noCompConn);
        await nativeNoComp.OpenAsync(default);
        using var http = ClickHouseHttp.ForFixture();

        await Workload.ResetTableAsync(driver);

        // All payloads built once, outside the measured region, so each route times only its own insert.
        Console.WriteLine($"Preparing payloads for {rows:N0} rows (native rows + server-minted Parquet/RowBinary)...");
        var typedRows = BulkRow.Build(rows);
        var parquet = await http.QueryBytesAsync(GenSql(rows, "Parquet"));
        var rowBinary = await http.QueryBytesAsync(GenSql(rows, "RowBinary"));
        await ClickHouseFixture.CopyFileToServerAsync(parquet, ParquetWorkload.ServerFileName);

        var cases = new (string Label, Func<Task<long>> Run)[]
        {
            ("CH.Native streaming (LZ4 wire)",
                () => NativeBulkInsertRunner.RunTypedAsync(native, typedRows)),

            ("CH.Native streaming (no compression)",
                () => NativeBulkInsertRunner.RunTypedAsync(nativeNoComp, typedRows)),

            ("HTTP RowBinary (single INSERT)",
                async () =>
                {
                    await http.PostBodyAsync($"INSERT INTO {Workload.Table} FORMAT RowBinary", rowBinary);
                    return (long)rows;
                }),

            ("HTTP Parquet (single INSERT)",
                () => ParquetWorkload.PostInsertAsync(http, parquet, rows)),

            ("Parquet file() (server-local, pre-staged)",
                () => ParquetWorkload.FileIngestAsync(driver, rows)),
        };

        Console.WriteLine($"\n=== Server-side load | {rows:N0} rows, single INSERT | {warmup} warmup + {reps} reps ===");
        Console.WriteLine($"{"Route",-42}{"client wall",13}{"server dur",12}{"server CPU",12}{"net-recv",11}{"peak mem",11}");
        Console.WriteLine(new string('-', 42 + 13 + 12 + 12 + 11 + 11));

        foreach (var (label, run) in cases)
        {
            List<double> wall = new(), dur = new(), cpu = new(), net = new(), mem = new();
            for (var i = 0; i < warmup + reps; i++)
            {
                await Workload.TruncateAsync(driver);
                var sw = Stopwatch.StartNew();
                var written = await run();
                sw.Stop();
                if (written != rows)
                    throw new InvalidOperationException($"{label}: wrote {written}, expected {rows}");

                var s = await ServerQueryLog.LatestInsertAsync(driver);
                if (i >= warmup)
                {
                    wall.Add(sw.Elapsed.TotalMilliseconds);
                    dur.Add(s.QueryDurationMs);
                    cpu.Add(s.CpuMs);
                    net.Add(s.NetworkRecvMs);
                    mem.Add(s.PeakMemoryBytes / 1e6);
                }
            }

            Console.WriteLine(
                $"{label,-42}{Med(wall),10:N0} ms{Med(dur),9:N0} ms{Med(cpu),9:N0} ms{Med(net),8:N0} ms{Med(mem),8:N0} MB");
        }

        Console.WriteLine();
        Console.WriteLine("server CPU = ProfileEvents['OSCPUVirtualTimeMicroseconds'] (query-thread CPU).");
        Console.WriteLine("Native vs RowBinary is the clean, fully-attributed CPU comparison; Parquet decode runs");
        Console.WriteLine("partly on background parser threads, so its server CPU is UNDER-counted — read its");
        Console.WriteLine("server duration, not its CPU. net-recv = time the server sat blocked reading client bytes");
        Console.WriteLine("(low for native = client production overlaps server ingestion).");
    }

    private static string GenSql(int rows, string format) =>
        "SELECT toInt64(number) AS id, concat('BulkItem_', toString(number)) AS name, " +
        $"number * 1.5 AS value FROM numbers({rows}) FORMAT {format}";

    private static double Med(List<double> xs)
    {
        if (xs.Count == 0) return 0;
        xs.Sort();
        return xs[xs.Count / 2];
    }
}
