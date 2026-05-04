<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Agent Guidelines for Apache DataFusion Comet

## Developer Documentation

- [Contributor Guide](docs/source/contributor-guide/index.md)
- [Development Guide](docs/source/contributor-guide/development.md) (build, test, common pitfalls)
- [Spark SQL Tests](docs/source/contributor-guide/spark-sql-tests.md) (working with Spark diffs)
- [Adding a New Expression](docs/source/contributor-guide/adding_a_new_expression.md)
- [Adding a New Operator](docs/source/contributor-guide/adding_a_new_operator.md)
- [Debugging Guide](docs/source/contributor-guide/debugging.md)

## Building and Testing

Comet is a mixed JVM/native project: Scala/Java code in `common/` and `spark/`, Rust code in
`native/`. The two sides communicate via JNI and Arrow FFI, and the build order matters.

### Build order

The native library must be built before any JVM tests, and `make` must have been run at least
once after cloning so that protobuf classes are generated for the JVM side.

```bash
make            # First-time setup: builds native + JVM, generates protobuf
make core       # Rebuild native Rust code only (debug)
make test-jvm   # Builds native first, then runs JVM tests
make test-rust  # Builds common JVM first (for CometException), then runs cargo test
make test       # Runs both Rust and JVM tests
```

If you edit Rust code and run JVM tests without rebuilding native, the tests will exercise the
old library. When in doubt, run `make` again.

### Running specific JVM tests

Use `-Dsuites` with the fully qualified class name, and `-Dtest=none` to skip the JUnit runner:

```bash
./mvnw test -Dtest=none -Dsuites="org.apache.comet.CometCastSuite"
./mvnw test -Dtest=none -Dsuites="org.apache.comet.CometCastSuite valid"   # filter by name
```

Do **not** use `-pl spark` (or any other `-pl`) to scope a Maven build. It causes Maven to
resolve sibling modules from the local repository, which is often stale, and you will silently
test against an old `common`. See the
[Avoid Using `-pl` to Select Modules](docs/source/contributor-guide/development.md#avoid-using--pl-to-select-modules)
section for details.

Spark version is selected via Maven profile: `-Pspark-3.4`, `-Pspark-3.5`, `-Pspark-4.0`
(default 3.5).

### Running Rust tests directly

`cargo test` needs `libjvm` on the library path:

```bash
export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$LD_LIBRARY_PATH
cd native && cargo test
```

`make test-rust` handles this for you.

## Working with Spark Diffs

Comet runs the upstream Spark SQL test suite against itself, using patch files under
`dev/diffs/` (one per supported Spark version, for example `dev/diffs/3.5.6.diff`). These files
are large generated diffs of an Apache Spark source tree.

**Never edit files under `dev/diffs/` by hand.** A hand-edited diff almost always becomes
malformed (line counts, hunk headers, context drift) and silently fails to apply, or applies
incorrectly. The correct workflow is:

1. Clone Apache Spark at the matching tag.
2. Apply the existing diff with `git apply`.
3. Modify the Spark source tree.
4. Regenerate the diff with `git diff <tag> > dev/diffs/<version>.diff`.

The full procedure, including how to set `core.abbrev` and which untracked files must be
`git add`ed before generating the diff, is documented in
[Spark SQL Tests: Creating a diff file for a new Spark version](docs/source/contributor-guide/spark-sql-tests.md#creating-a-diff-file-for-a-new-spark-version)
and
[Generating The Diff File](docs/source/contributor-guide/spark-sql-tests.md#generating-the-diff-file).

## Before Committing

At a minimum, run these and fix any errors before committing:

```bash
make format                                                  # cargo fmt + scalafix + spotless
make                                                         # full build
cd native && cargo clippy --all-targets --workspace -- -D warnings
```

Then run the relevant subset of tests for your change. Avoid running the full JVM suite locally;
let CI handle it.

When opening a pull request, use the
[PR template](.github/pull_request_template.md) and fill in every section.
