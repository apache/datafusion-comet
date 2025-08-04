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

# fs-hdfs3

It's based on the version ``0.0.4`` of http://hyunsik.github.io/hdfs-rs to provide libhdfs binding library and rust APIs which safely wraps libhdfs binding APIs.

# Current Status
* All libhdfs FFI APIs are ported.
* Safe Rust wrapping APIs to cover most of the libhdfs APIs except those related to zero-copy read.
* Compared to hdfs-rs, it removes the lifetime in HdfsFs, which will be more friendly for others to depend on.

## Documentation
* [API documentation] (https://docs.rs/crate/fs-hdfs3)

## Requirements
* The C related files are from the branch ``3.1.4`` of hadoop repository. For rust usage, a few changes are also applied.
* No need to compile the Hadoop native library by yourself. However, the Hadoop jar dependencies are still required.

## Usage
Add this to your Cargo.toml:

```toml
[dependencies]
fs-hdfs3 = "0.1.12"
```

### Build

We need to specify ```$JAVA_HOME``` to make Java shared library available for building.

### Run
Since our compiled libhdfs is JNI-based implementation, 
it requires Hadoop-related classes available through ``CLASSPATH``. An example,

```sh
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`
```

Also, we need to specify the JVM dynamic library path for the application to load the JVM shared library at runtime.

For jdk8 and macOS, it's

```sh
export DYLD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server
```

For jdk11 (or later jdks) and macOS, it's

```sh
export DYLD_LIBRARY_PATH=$JAVA_HOME/lib/server
```

For jdk8 and Centos
```sh
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server
```

For jdk11 (or later jdks) and Centos
```sh
export LD_LIBRARY_PATH=$JAVA_HOME/lib/server
```

### Testing
The test also requires the ``CLASSPATH`` and `DYLD_LIBRARY_PATH` (or `LD_LIBRARY_PATH`). In case that the java class of ``org.junit.Assert`` can't be found. Refine the ``$CLASSPATH`` as follows:

```sh
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`:$HADOOP_HOME/share/hadoop/tools/lib/*
```

Here, ``$HADOOP_HOME`` need to be specified and exported.

Then you can run

```bash
cargo test
```

## Example

```rust
use std::sync::Arc;
use hdfs::hdfs::{get_hdfs_by_full_path, HdfsFs};

let fs: Arc<HdfsFs> = get_hdfs_by_full_path("hdfs://localhost:8020/").ok().unwrap();
match fs.mkdir("/data") {
    Ok(_) => { println!("/data has been created") },
    Err(_)  => { panic!("/data creation has failed") }
};
```