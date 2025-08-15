# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache DataFusion Comet is a high-performance accelerator for Apache Spark, built on top of the Apache DataFusion query engine. The project combines Rust native code with Scala/Java JVM code to deliver significant performance improvements for Spark workloads.

## Build System & Commands

This is a multi-language project using Maven for JVM components and Cargo for Rust components.

### Main Build Commands

- `make all` - Build both native Rust and JVM components
- `make core` - Build native Rust code only (`cd native && cargo build`)
- `make jvm` - Build JVM components only (`./mvnw clean package -DskipTests`)
- `make release` - Create optimized release build
- `make clean` - Clean all build artifacts

### Testing Commands

- `make test` - Run all tests (both Rust and JVM)
- `make test-rust` - Run Rust tests only
- `make test-jvm` - Run JVM tests only
- `./mvnw verify` - Run JVM integration tests
- `cd native && cargo test` - Run Rust unit tests directly

### Code Quality Commands

- `make format` - Format all code (Rust with `cargo fmt`, Scala with `spotless:apply`)
- `./mvnw spotless:check` - Check code formatting
- `./mvnw scalastyle:check` - Run Scala style checks

### Benchmarking

- `make bench` - Run Rust benchmarks
- `make benchmark-<ClassName>` - Run specific JVM benchmark class

## Architecture

### High-Level Structure

The project consists of two main components:

1. **Native Rust Code** (`native/` directory)
   - Core execution engine built on Apache DataFusion
   - JNI interfaces for communication with JVM
   - Performance-critical operations like Parquet reading, expression evaluation, and columnar operations

2. **JVM Scala/Java Code** (`spark/`, `common/` directories)
   - Spark plugin integration
   - Query plan translation and optimization
   - JVM-side memory management and coordination

### Key Directories

- `native/core/` - Main Rust execution engine
- `native/spark-expr/` - Rust implementations of Spark expressions
- `native/proto/` - Protocol buffer definitions for JVM-Rust communication
- `native/fs-hdfs/` - HDFS filesystem support
- `spark/` - Main Spark plugin code
- `common/` - Shared utilities between Spark versions
- `fuzz-testing/` - Property-based testing framework

### Native Code Organization

- `native/core/src/execution/` - Main execution operators (scan, shuffle, etc.)
- `native/core/src/parquet/` - Parquet file format support
- `native/core/src/jvm_bridge/` - JNI interfaces
- `native/spark-expr/src/` - Expression implementations organized by category (math, string, datetime, etc.)

## Development Workflow

### Spark Version Support

The project supports multiple Spark versions through profile-based builds:
- Default: Spark 3.5 with Scala 2.12
- Profiles: `spark-3.4`, `spark-3.5`, `spark-4.0`
- Scala versions: `scala-2.12`, `scala-2.13`

Use `PROFILES` environment variable or `-P` flag:
```bash
./mvnw clean package -Pspark-3.4
PROFILES="-Pspark-4.0 -Pscala-2.13" make test-jvm
```

### JNI Development

Native library location is configured via Maven properties:
- Debug builds: `native/target/debug/`
- Release builds: `native/target/release/`

The JVM code loads native libraries from these locations automatically.

### Testing Strategy

1. **Unit Tests**: Both Rust (`cargo test`) and Scala/Java (ScalaTest, JUnit)
2. **Integration Tests**: Full Spark SQL queries with TPC-H/TPC-DS benchmarks
3. **Plan Stability Tests**: Ensure query plans remain consistent across versions
4. **Fuzz Testing**: Property-based testing for expression correctness

### Performance Considerations

- Native code is optimized for specific CPU features (`RUSTFLAGS="-Ctarget-cpu=native"`)
- Memory management uses Apache Arrow's columnar format
- JNI calls are minimized through batch processing
- Custom memory pools prevent fragmentation

## Common Patterns

### Adding New Expressions

1. Add Rust implementation in `native/spark-expr/src/`
2. Add protobuf definition in `native/proto/src/proto/`
3. Add JVM-side expression in appropriate `spark-*/` directory
4. Update expression mapping in query planner

### Adding New Operators

1. Implement in `native/core/src/execution/operators/`
2. Add JNI bindings in `native/core/src/jvm_bridge/`
3. Add Spark physical operator in JVM code
4. Register in `CometSparkSessionExtensions`

### Debugging

- Use `RUST_BACKTRACE=1` for detailed Rust stack traces
- JVM debugging via standard Java debugging tools
- Native profiling tools like `perf` work with release builds (debug info included)

## Dependencies

### Major Rust Dependencies
- `datafusion` - Core query execution engine
- `arrow` - Columnar data format
- `parquet` - Parquet file format support
- `object_store` - Cloud storage abstraction

### Major JVM Dependencies
- Apache Spark (3.4, 3.5, or 4.0)
- Apache Arrow Java
- Apache Parquet Java
- ScalaTest for testing

## Build Profiles & Cross-Compilation

The project supports cross-compilation for multiple platforms:
- Linux: x86_64, aarch64
- macOS: x86_64, aarch64 (Apple Silicon)
- Windows: x86_64

Release builds can target multiple architectures simultaneously using the `release-linux` target.