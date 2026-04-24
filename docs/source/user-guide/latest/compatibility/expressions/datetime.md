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
  timezone is UTC. TimestampNTZ inputs are handled correctly (timezone-independent truncation).
  [#2649](https://github.com/apache/datafusion-comet/issues/2649)

## Date and Time Functions

Comet's native implementation of date and time functions may produce different results than Spark for dates
far in the future (approximately beyond year 2100). This is because Comet uses the chrono-tz library for
timezone calculations, which has limited support for Daylight Saving Time (DST) rules beyond the IANA
time zone database's explicit transitions.

For dates within a reasonable range (approximately 1970-2100), Comet's date and time functions are compatible
with Spark. For dates beyond this range, functions that involve timezone-aware calculations (such as
`date_trunc` with timezone-aware timestamps) may produce results with incorrect DST offsets.

If you need to process dates far in the future with accurate timezone handling, consider:

- Using timezone-naive types (`timestamp_ntz`) when timezone conversion is not required
- Falling back to Spark for these specific operations
 
<!--BEGIN:EXPR_COMPAT[datetime]-->

<!--END:EXPR_COMPAT-->
