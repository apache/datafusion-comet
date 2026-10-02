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

- **TruncTimestamp (date_trunc)**: In non-UTC sessions the native path is marked Incompatible and
  routes through the JVM codegen dispatcher by default, producing Spark-identical results. The
  native path is itself correct for dates within chrono-tz's DST horizon (approximately year 2100;
  see "Date and Time Functions" below) and can be enabled by setting
  `spark.comet.expression.TruncTimestamp.allowIncompatible=true`. TimestampNTZ inputs are handled
  correctly regardless of session timezone (timezone-independent truncation).

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

### Timezone Database Versions

Comet's native code converts between instants and local time with the IANA timezone database that
chrono-tz compiles into the Comet library. Spark uses the JVM's timezone database (`tzdb.dat`), whose
version depends on the JDK build and on whether its timezone data has been updated. When the two versions
have different rules for a timezone, local times that Comet computes natively for that timezone can differ
from Spark's for the affected dates. That covers `hour`, casts between timestamps and strings or dates,
`date_trunc`, and parsing strings as timestamps. Comet logs a warning at startup when the two versions
differ.

<!--BEGIN:EXPR_COMPAT[datetime]-->

<!--END:EXPR_COMPAT-->
