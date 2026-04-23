<!---
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

# Array Expressions

- **ArrayUnion**: Sorts input arrays before performing the union, while Spark preserves the order of the first array
  and appends unique elements from the second.
  [#3644](https://github.com/apache/datafusion-comet/issues/3644)
- **SortArray**: Nested arrays with `Struct` or `Null` child values are not supported natively and will fall back to Spark.
