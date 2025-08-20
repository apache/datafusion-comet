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

# Comet Roadmap

Comet is an open-source project and contributors are welcome to work on any issues at any time, but we find it 
helpful to have a roadmap for some of the major items that require coordination between contributors.

## Major Initiatives

### Iceberg Integration

Iceberg integration is still a work-in-progress ([#2060]), with major improvements expected in the next few
releases. Once this integration is complete, we plan on switching from the `native_comet` scan to the 
`native_iceberg_compat` scan ([#2189]) so that complex types can be supported.

[#2060]: https://github.com/apache/datafusion-comet/issues/2060
[#2189]: https://github.com/apache/datafusion-comet/issues/2189

### Spark 4.0.0 Support

Comet has experimental support for Spark 4.0.0, but there is more work to do ([#1637]), such as enabling 
more Spark SQL tests and fully implementing ANSI support ([#313]) for all supported expressions.

[#313]: https://github.com/apache/datafusion-comet/issues/313
[#1637]: https://github.com/apache/datafusion-comet/issues/1637

### Removing the native_comet scan implementation

We are working towards deprecating ([#2186]) and removing ([#2177]) the `native_comet` scan implementation, which
is the originally scan implementation that uses mutable buffers (which is incompatible with best practices around
Arrow FFI) and does not support complex types.

Once we are using the `native_iceberg_compat` scan (which is based on DataFusion's `DataSourceExec`) in the Iceberg 
integration, we will be able to remove the `native_comet` scan implementation, and can then improve the efficiency 
of our use of Arrow FFI ([#2171]). 

[#2186]: https://github.com/apache/datafusion-comet/issues/2186
[#2171]: https://github.com/apache/datafusion-comet/issues/2171
[#2177]: https://github.com/apache/datafusion-comet/issues/2177

## Ongoing Improvements

In addition to the major initiatives above, we have the following ongoing areas of work: 

- Adding support for more Spark expressions
- Moving more expressions to the `datafusion-spark` crate in the core DataFusion repository
- Performance tuning
- Nested type support improvements