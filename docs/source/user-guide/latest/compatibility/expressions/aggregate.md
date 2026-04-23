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

# Aggregate Expressions

<!--BEGIN:EXPR_COMPAT[aggregate]-->
<!--END:EXPR_COMPAT-->

## ANSI Mode

Comet will fall back to Spark for some aggregate expressions when ANSI mode is enabled. These can be enabled by setting `spark.comet.expression.EXPRNAME.allowIncompatible=true`, where `EXPRNAME` is the Spark expression class name. See the [Comet Supported Expressions Guide](../../expressions.md) for more information on this configuration setting.

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.
