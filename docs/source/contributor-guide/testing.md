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

# Testing

## Identifying Edge Cases

## Scala Unit Tests

The Scala unit tests generally test for correctness by running the same query against Spark with Comet enabled 
and disabled, and compare the results.

The `checkSparkAnswerAndOperator` method can be used to also confirm that all operators and expressions 
were accelerated by Comet.

If the plan is expected to contain a mix of Spark and Comet operators, it is possible to pass lists of expected classes that should be included or excluded:

```scala
checkSparkAnswerAndOperator(df, 
  includeClasses = Seq(classOf[CometBroadcastExchangeExec], classOf[CometBroadcastHashJoinExec]))
```

For cases where Comet is expected to fall back to Spark, `checkSparkAnswerAndFallbackReason` can be used instead.


## Rust Unit Tests

## Fuzz Testing

