# Per-Partition Plan Building for Native Iceberg Scans

## Summary

This PR ensures that when Iceberg scans are executed natively, each Spark task's native plan only contains file scan tasks for that specific partition, rather than all partitions.

## Problem

Previously, when `CometIcebergNativeScanExec` was a child of another operator (e.g., `CometFilterExec`), the parent operator's serialized native plan (`nativeOp`) contained the entire `IcebergScan` message with data for all partitions. This meant:

- Each task received metadata for all partitions when it only needed one
- The serialized plan size grew linearly with partition count
- Every task deserialized unnecessary partition data

## Solution

This PR implements per-partition plan building that injects only the relevant partition's data at execution time:

### New Components

**`CometIcebergSplitRDD`** - A custom RDD that:
- Holds common data (deduplication pools, catalog properties) in the closure
- Stores each partition's file scan tasks in its `Partition` objects
- Combines common + partition data at compute time to build partition-specific native plans

**`IcebergPartitionInjector`** - A helper that traverses an `Operator` tree and injects partition data into `IcebergScan` nodes that are missing it

**`findIcebergSplitData()`** - Locates `CometIcebergNativeScanExec` descendants in the plan tree and retrieves their per-partition data

### Modified Execution Flow

In `CometNativeExec.doExecuteColumnar()`:
1. Check if the plan tree contains an `IcebergScan` with per-partition data available
2. If so, for each partition:
   - Parse the base operator tree from the serialized plan
   - Inject that partition's file scan task data into the `IcebergScan` node
   - Re-serialize and pass to native execution
3. Each task's native plan now only contains its own partition's data

### Protobuf Changes

Added `IcebergScanCommon` message to hold shared data (pools, metadata) separately from per-partition file scan tasks. The `IcebergScan` message now has:
- `common` field for shared deduplication pools
- `partition` field for a single partition's file tasks

### Rust Changes

Simplified the Iceberg scan handling in `planner.rs` to expect common + partition data, removing the code path that read from a list of all partitions.

## Test Plan

- [x] All 62 existing Iceberg tests pass
- [x] Filter pushdown tests verify parent operators work correctly
- [x] MOR (Merge-On-Read) tests with positional and equality deletes
- [x] Schema evolution, complex types, partition pruning tests
- [x] REST catalog integration test

---

Generated with [Claude Code](https://claude.ai/code)
