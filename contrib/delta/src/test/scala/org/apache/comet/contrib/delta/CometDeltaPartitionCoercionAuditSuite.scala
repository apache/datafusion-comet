/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.contrib.delta

import org.apache.spark.sql.functions._

// Audit 4: partition-value coercion through the JVM->native boundary.
//
// Delta stores partition values as STRINGS in the log (DeltaLog.AddFile's
// partitionValues: Map[String,String]). On read, those strings must be
// parsed back to the partition column's logical type. Most regressions
// in this area are silent: a date parsed as the wrong epoch, a decimal
// losing precision, a timestamp picking up the JVM default timezone.
//
// For each partition-value-bearing type, write rows under a partition
// boundary value and assert both data rows and a partition-pruned
// filter match vanilla Spark.
class CometDeltaPartitionCoercionAuditSuite extends CometDeltaTestBase {

  test("partition by DATE: min/max + filter pruning matches vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_date") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, dt DATE)
           |USING delta PARTITIONED BY (dt)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, DATE'1970-01-01'),
           |(2, DATE'2026-05-23'),
           |(3, DATE'9999-12-31'),
           |(4, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
      // Partition pruning: filter on partition column
      assertDeltaNativeMatches(
        tablePath,
        _.filter(col("dt") === lit("2026-05-23").cast("date")).orderBy("id"))
    }
  }

  test("partition by TIMESTAMP: UTC and non-UTC values match vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_ts") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, ts TIMESTAMP)
           |USING delta PARTITIONED BY (ts)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, TIMESTAMP'2026-01-01 00:00:00 UTC'),
           |(2, TIMESTAMP'2026-05-23 12:34:56 UTC'),
           |(3, TIMESTAMP'2026-12-31 23:59:59.999999 UTC'),
           |(4, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("partition by TIMESTAMP_NTZ: no timezone shift") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_tsntz") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, ts TIMESTAMP_NTZ)
           |USING delta PARTITIONED BY (ts)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, TIMESTAMP_NTZ'2026-05-23 00:00:00'),
           |(2, TIMESTAMP_NTZ'2026-05-23 23:59:59.123456'),
           |(3, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("partition by DECIMAL: precision and sign preserved") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_dec") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, d DECIMAL(18,6))
           |USING delta PARTITIONED BY (d)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, CAST(0 AS DECIMAL(18,6))),
           |(2, CAST(-999999999999.999999 AS DECIMAL(18,6))),
           |(3, CAST(999999999999.999999 AS DECIMAL(18,6))),
           |(4, CAST(0.000001 AS DECIMAL(18,6))),
           |(5, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("partition by BIGINT: full range preserved") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_long") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (v STRING, k BIGINT)
           |USING delta PARTITIONED BY (k)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |('min', -9223372036854775808),
           |('zero', 0),
           |('max', 9223372036854775807),
           |('null', NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("v"))
    }
  }

  test("partition by STRING with special chars (URL-encoded path)") {
    // Delta path-escapes partition values; bb0686c1/9ab8b842 fixed the
    // double-encoding case for DV stores. This locks in that the basic
    // read still works for spaces/percents/etc.
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_str") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, p STRING)
           |USING delta PARTITIONED BY (p)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, 'plain'),
           |(2, 'has space'),
           |(3, 'percent%2a'),
           |(4, ''),
           |(5, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("partition by BOOLEAN") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_bool") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (id INT, b BOOLEAN)
           |USING delta PARTITIONED BY (b)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, true), (2, false), (3, NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
    }
  }

  test("multi-column partition: (dt, region) IsNull pruning") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_multi") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath`
           |  (id INT, dt DATE, region STRING)
           |USING delta PARTITIONED BY (dt, region)""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, DATE'2026-05-23', 'us'),
           |(2, DATE'2026-05-23', 'eu'),
           |(3, DATE'2026-05-24', 'us'),
           |(4, NULL, 'us'),
           |(5, DATE'2026-05-23', NULL)""".stripMargin)
      assertDeltaNativeMatches(tablePath, _.orderBy("id"))
      // Pruning: dt = 2026-05-23 AND region IS NULL
      assertDeltaNativeMatches(
        tablePath,
        _.filter(col("dt") === lit("2026-05-23").cast("date") &&
          col("region").isNull).orderBy("id"))
    }
  }

  test("partition column timezone-sensitive read: session TZ swap") {
    // TIMESTAMP partition values are stored in UTC normalized form by
    // Delta; native must not double-apply the session timezone.
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_tz") { tablePath =>
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(
          s"""CREATE TABLE delta.`$tablePath` (id INT, ts TIMESTAMP)
             |USING delta PARTITIONED BY (ts)""".stripMargin)
        spark.sql(
          s"""INSERT INTO delta.`$tablePath` VALUES
             |(1, TIMESTAMP'2026-05-23 00:00:00 UTC'),
             |(2, TIMESTAMP'2026-05-23 12:00:00 UTC')""".stripMargin)
      }
      // Now read under a different TZ.
      withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
        assertDeltaNativeMatches(tablePath, _.orderBy("id"))
      }
    }
  }
}
