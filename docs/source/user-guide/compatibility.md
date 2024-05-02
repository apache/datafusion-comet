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

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide offers information about areas of functionality where there are known differences.

## ANSI mode

Comet currently ignores ANSI mode in most cases, and therefore can produce different results than Spark. By default,
Comet will fall back to Spark if ANSI mode is enabled. To enable Comet to accelerate queries when ANSI mode is enabled,
specify `spark.comet.ansi.enabled=true` in the Spark configuration. Comet's ANSI support is experimental and should not
be used in production.

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## Cast

Cast operations in Comet fall into three levels of support:

- Compatible: The results match Apache Spark
- Incompatible: The results may match with Apache Spark for some inputs, but there are known issues where some  
   inputs will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- Unsupported: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.

| From Type | To Type   | Compatible?  | Notes                               |
| --------- | --------- | ------------ | ----------------------------------- |
| boolean   | boolean   | Compatible   |                                     |
| boolean   | byte      | Compatible   |                                     |
| boolean   | short     | Compatible   |                                     |
| boolean   | integer   | Compatible   |                                     |
| boolean   | long      | Compatible   |                                     |
| boolean   | float     | Compatible   |                                     |
| boolean   | double    | Compatible   |                                     |
| boolean   | decimal   | Unsupported  |                                     |
| boolean   | string    | Compatible   |                                     |
| boolean   | timestamp | Unsupported  |                                     |
| byte      | boolean   | Compatible   |                                     |
| byte      | byte      | Compatible   |                                     |
| byte      | short     | Compatible   |                                     |
| byte      | integer   | Compatible   |                                     |
| byte      | long      | Compatible   |                                     |
| byte      | float     | Compatible   |                                     |
| byte      | double    | Compatible   |                                     |
| byte      | decimal   | Compatible   |                                     |
| byte      | string    | Compatible   |                                     |
| byte      | binary    | Unsupported  |                                     |
| byte      | timestamp | Unsupported  |                                     |
| short     | boolean   | Compatible   |                                     |
| short     | byte      | Compatible   |                                     |
| short     | short     | Compatible   |                                     |
| short     | integer   | Compatible   |                                     |
| short     | long      | Compatible   |                                     |
| short     | float     | Compatible   |                                     |
| short     | double    | Compatible   |                                     |
| short     | decimal   | Compatible   |                                     |
| short     | string    | Compatible   |                                     |
| short     | binary    | Unsupported  |                                     |
| short     | timestamp | Unsupported  |                                     |
| integer   | boolean   | Compatible   |                                     |
| integer   | byte      | Compatible   |                                     |
| integer   | short     | Compatible   |                                     |
| integer   | integer   | Compatible   |                                     |
| integer   | long      | Compatible   |                                     |
| integer   | float     | Compatible   |                                     |
| integer   | double    | Compatible   |                                     |
| integer   | decimal   | Compatible   |                                     |
| integer   | string    | Compatible   |                                     |
| integer   | binary    | Unsupported  |                                     |
| integer   | timestamp | Unsupported  |                                     |
| long      | boolean   | Compatible   |                                     |
| long      | byte      | Compatible   |                                     |
| long      | short     | Compatible   |                                     |
| long      | integer   | Compatible   |                                     |
| long      | long      | Compatible   |                                     |
| long      | float     | Compatible   |                                     |
| long      | double    | Compatible   |                                     |
| long      | decimal   | Compatible   |                                     |
| long      | string    | Compatible   |                                     |
| long      | binary    | Unsupported  |                                     |
| long      | timestamp | Unsupported  |                                     |
| float     | boolean   | Compatible   |                                     |
| float     | byte      | Unsupported  |                                     |
| float     | short     | Unsupported  |                                     |
| float     | integer   | Unsupported  |                                     |
| float     | long      | Unsupported  |                                     |
| float     | float     | Compatible   |                                     |
| float     | double    | Compatible   |                                     |
| float     | decimal   | Unsupported  |                                     |
| float     | string    | Incompatible |                                     |
| float     | timestamp | Unsupported  |                                     |
| double    | boolean   | Compatible   |                                     |
| double    | byte      | Unsupported  |                                     |
| double    | short     | Unsupported  |                                     |
| double    | integer   | Unsupported  |                                     |
| double    | long      | Unsupported  |                                     |
| double    | float     | Compatible   |                                     |
| double    | double    | Compatible   |                                     |
| double    | decimal   | Incompatible |                                     |
| double    | string    | Incompatible |                                     |
| double    | timestamp | Unsupported  |                                     |
| decimal   | boolean   | Unsupported  |                                     |
| decimal   | byte      | Unsupported  |                                     |
| decimal   | short     | Unsupported  |                                     |
| decimal   | integer   | Unsupported  |                                     |
| decimal   | long      | Unsupported  |                                     |
| decimal   | float     | Compatible   |                                     |
| decimal   | double    | Compatible   |                                     |
| decimal   | decimal   | Compatible   |                                     |
| decimal   | string    | Unsupported  |                                     |
| decimal   | timestamp | Unsupported  |                                     |
| string    | boolean   | Compatible   |                                     |
| string    | byte      | Compatible   |                                     |
| string    | short     | Compatible   |                                     |
| string    | integer   | Compatible   |                                     |
| string    | long      | Compatible   |                                     |
| string    | float     | Unsupported  |                                     |
| string    | double    | Unsupported  |                                     |
| string    | decimal   | Unsupported  |                                     |
| string    | string    | Compatible   |                                     |
| string    | binary    | Compatible   |                                     |
| string    | date      | Unsupported  |                                     |
| string    | timestamp | Incompatible | Not all valid formats are supported |
| binary    | string    | Incompatible |                                     |
| binary    | binary    | Compatible   |                                     |
| date      | boolean   | Unsupported  |                                     |
| date      | byte      | Unsupported  |                                     |
| date      | short     | Unsupported  |                                     |
| date      | integer   | Unsupported  |                                     |
| date      | long      | Unsupported  |                                     |
| date      | float     | Unsupported  |                                     |
| date      | double    | Unsupported  |                                     |
| date      | decimal   | Unsupported  |                                     |
| date      | string    | Compatible   |                                     |
| date      | date      | Compatible   |                                     |
| date      | timestamp | Unsupported  |                                     |
| timestamp | boolean   | Unsupported  |                                     |
| timestamp | byte      | Unsupported  |                                     |
| timestamp | short     | Unsupported  |                                     |
| timestamp | integer   | Unsupported  |                                     |
| timestamp | long      | Compatible   |                                     |
| timestamp | float     | Unsupported  |                                     |
| timestamp | double    | Unsupported  |                                     |
| timestamp | decimal   | Unsupported  |                                     |
| timestamp | string    | Compatible   |                                     |
| timestamp | date      | Compatible   |                                     |
| timestamp | timestamp | Compatible   |                                     |
