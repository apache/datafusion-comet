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

# Supported Spark Data Sources

## Parquet

When `spark.comet.scan.enabled` is enabled, Parquet scans will be performed natively by Comet if all data types
in the schema are supported. When this option is not enabled, the scan will fall back to Spark. In this case,
enabling `spark.comet.convert.parquet.enabled` will immediately convert the data into Arrow format, allowing native 
execution to happen after that, but the process may not be efficient.

## CSV

Comet does not provide native CSV scan, but when `spark.comet.convert.csv.enabled` is enabled, data is immediately
converted into Arrow format, allowing native execution to happen after that.

## JSON

Comet does not provide native JSON scan, but when `spark.comet.convert.json.enabled` is enabled, data is immediately
converted into Arrow format, allowing native execution to happen after that.
