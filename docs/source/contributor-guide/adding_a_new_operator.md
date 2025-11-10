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

# Adding a New Expression

`CometExecRule` is responsible for replacing Spark operators with Comet operators.

There are some different approaches to implement Comet operators.

## Comet Native Operators

For operators that can run entirely natively, `CometExecRule` currently delegates to `QueryPlanSerde.operator2Proto` 
to perform checks to see if the operator is enabled or disabled via configs and will also check if the operator can be 
supported and will tag the operator with fallback reasons if there are any.

## Comet JVM Operators

Some Comet operators run in the JVM. For thede operators, all checks happen in `CometExecRule` rather 
than `QueryPlanSerde`, because these operators do not need to be serialized to protobuf.

Some examples of Comet JVM Operators are `CometBroadcastExchangeExec` and `CometShuffleExchangeExec`.

## Comet Sinks

Some operators can be sinks, meaning that they can be read as the leaf node of a native plan from `ScanExec`.

Some examples of sinks are `CometScanExec`, `UnionExec`, and `CometSparkToColumnarExec`.

