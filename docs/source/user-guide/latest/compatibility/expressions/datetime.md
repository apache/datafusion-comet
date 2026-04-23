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

# Date/Time Expressions

- **Hour, Minute, Second**: Incorrectly apply timezone conversion to TimestampNTZ inputs. TimestampNTZ stores local
  time without timezone, so no conversion should be applied. These expressions work correctly with Timestamp inputs.
  [#3180](https://github.com/apache/datafusion-comet/issues/3180)
- **TruncTimestamp (date_trunc)**: Produces incorrect results when used with non-UTC timezones. Compatible when
  timezone is UTC.
  [#2649](https://github.com/apache/datafusion-comet/issues/2649)
