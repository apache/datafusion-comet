# datafusion-comet-ballista

Lets Apache DataFusion Ballista execute Apache DataFusion Comet native plans
that are handed across a `datafusion-ffi` boundary, so a Ballista executor can
run Comet's native scans without linking Comet's Rust crates directly.

- [`scan::CometScanExec`] — a serializable DataFusion leaf that carries a
  Comet plan's proto bytes and rebuilds the FFI plan at `execute()` time. This
  is what Ballista ships to executors and reconstructs there.
- [`codec::CometPhysicalCodec`] / [`codec::CometLogicalCodec`] — extension
  codecs that (de)serialize Comet nodes as their proto bytes and delegate
  everything else to Ballista's own codecs.
- [`table_provider::CometTableProvider`] — a `TableProvider` that produces a
  `CometScanExec`, so a Comet scan can participate in a DataFusion logical
  plan and be distributed by Ballista like any other table.
