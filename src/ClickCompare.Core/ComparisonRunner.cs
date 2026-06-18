using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;

namespace ClickCompare.Core;

/// <summary>
/// Dispatches a <see cref="ComparisonCase"/> to the right client over a warm connection.
/// Both data shapes are passed in pre-materialised so row construction / boxing stays out of the
/// measured insert — the comparison is transport + serialization + server, not allocation.
/// </summary>
public static class ComparisonRunner
{
    public static Task<long> RunAsync(
        ComparisonCase c,
        DriverConnection driver,
        NativeConnection native,
        List<object[]> objectRows,
        List<BulkRow> typedRows,
        CancellationToken ct = default) =>
        c.Client switch
        {
            Client.Driver => BulkInsertRunner.RunAsync(
                driver, new InsertScenario(c.Name, c.RowCount, c.BatchSize, c.Mdop), objectRows, ct),
            Client.NativeTyped => NativeBulkInsertRunner.RunTypedAsync(native, typedRows, c.BatchSize, ct),
            Client.NativeDynamic => NativeBulkInsertRunner.RunDynamicAsync(native, objectRows, c.BatchSize, ct),
            Client.NativeTypedSplit => NativeBulkInsertRunner.RunTypedMultiInsertAsync(native, typedRows, c.BatchSize, ct),
            Client.NativeDynamicSplit => NativeBulkInsertRunner.RunDynamicMultiInsertAsync(native, objectRows, c.BatchSize, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(c)),
        };
}
