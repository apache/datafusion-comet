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

package org.apache.spark.sql.benchmark

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

/**
 * Configuration for a string expression benchmark.
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the Comet case
 */
case class StringExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet string expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometStringExpressionBenchmark-**results.txt". The
 * replace_match cases use literal arguments, while replace_column_match cases read the
 * replacement from a column. Each set uses the same input width with 0%, about 10%, and 100%
 * match density. Compare their Comet results on a dispatcher-only revision and a native-default
 * revision under the same Spark profile, batch size, and hardware.
 */
object CometStringExpressionBenchmark extends CometBenchmarkBase {

  // Configuration for all string expression benchmarks
  private val stringExpressions = List(
    StringExprConfig("ascii", "select ascii(c1) from parquetV1Table"),
    StringExprConfig("bit_length", "select bit_length(c1) from parquetV1Table"),
    StringExprConfig("btrim", "select btrim(c1) from parquetV1Table"),
    StringExprConfig("chr", "select chr(c1) from parquetV1Table"),
    StringExprConfig("concat", "select concat(c1, c1) from parquetV1Table"),
    StringExprConfig("concat_ws", "select concat_ws(' ', c1, c1) from parquetV1Table"),
    StringExprConfig("contains", "select contains(c1, '123') from parquetV1Table"),
    StringExprConfig("endswith", "select endswith(c1, '9') from parquetV1Table"),
    StringExprConfig("initCap", "select initCap(c1) from parquetV1Table"),
    StringExprConfig("instr", "select instr(c1, '123') from parquetV1Table"),
    StringExprConfig("length", "select length(c1) from parquetV1Table"),
    StringExprConfig("levenshtein", "select levenshtein(c1, 'test') from parquetV1Table"),
    StringExprConfig(
      "levenshtein_threshold",
      "select levenshtein(c1, 'test', 3) from parquetV1Table"),
    StringExprConfig("like", "select c1 like '%123%' from parquetV1Table"),
    StringExprConfig("lower", "select lower(c1) from parquetV1Table"),
    StringExprConfig("lpad", "select lpad(c1, 150, 'x') from parquetV1Table"),
    StringExprConfig("ltrim", "select ltrim(c1) from parquetV1Table"),
    StringExprConfig("octet_length", "select octet_length(c1) from parquetV1Table"),
    StringExprConfig(
      "regexp_replace",
      "select regexp_replace(c1, '[0-9]', 'X') from parquetV1Table"),
    StringExprConfig("repeat", "select repeat(c1, 3) from parquetV1Table"),
    StringExprConfig("replace", "select replace(c1, '123', 'ab') from parquetV1Table"),
    // Both literal arguments stay scalar in DataFusion 55.1.0's replace fast path.
    StringExprConfig(
      "replace_match_0pct",
      "select replace(replace_no_match, '123', 'ab') from parquetV1Table"),
    StringExprConfig(
      "replace_match_10pct",
      "select replace(replace_ten_percent, '123', 'ab') from parquetV1Table"),
    StringExprConfig(
      "replace_match_100pct",
      "select replace(replace_all_match, '123', 'ab') from parquetV1Table"),
    StringExprConfig(
      "replace_column_match_0pct",
      "select replace(replace_no_match, '123', replace_with) from parquetV1Table"),
    StringExprConfig(
      "replace_column_match_10pct",
      "select replace(replace_ten_percent, '123', replace_with) from parquetV1Table"),
    StringExprConfig(
      "replace_column_match_100pct",
      "select replace(replace_all_match, '123', replace_with) from parquetV1Table"),
    StringExprConfig("reverse", "select reverse(c1) from parquetV1Table"),
    StringExprConfig("rlike", "select c1 rlike '[0-9]+' from parquetV1Table"),
    StringExprConfig("rpad", "select rpad(c1, 150, 'x') from parquetV1Table"),
    StringExprConfig("rtrim", "select rtrim(c1) from parquetV1Table"),
    // `space` takes its length from a column rather than a literal. Given a literal, Comet's
    // native `space` receives a `ColumnarValue::Scalar`, builds one string per batch and lets
    // DataFusion broadcast it, while Spark's `StringSpace` calls `UTF8String.blankString` once
    // per row. The plans look symmetric but the work is not.
    StringExprConfig("space", "select space(c2) from parquetV1Table"),
    StringExprConfig("startswith", "select startswith(c1, '1') from parquetV1Table"),
    StringExprConfig("substring", "select substring(c1, 1, 100) from parquetV1Table"),
    StringExprConfig("translate", "select translate(c1, '123456', 'aBcDeF') from parquetV1Table"),
    StringExprConfig("trim", "select trim(c1) from parquetV1Table"),
    StringExprConfig("upper", "select upper(c1) from parquetV1Table")) ++
    Seq(16, 256, 1024).flatMap { searchBytes =>
      val search = "'" + ("a" * searchBytes) + "'"
      Seq(
        StringExprConfig(
          s"replace_column_search_${searchBytes}b_0pct",
          s"select replace(replace_long_${searchBytes}_0pct, $search, replace_with) " +
            "from parquetV1Table"),
        StringExprConfig(
          s"replace_column_search_${searchBytes}b_10pct",
          s"select replace(replace_long_${searchBytes}_10pct, $search, replace_with) " +
            "from parquetV1Table"),
        StringExprConfig(
          s"replace_column_search_${searchBytes}b_100pct",
          s"select replace(replace_long_${searchBytes}_100pct, $search, replace_with) " +
            "from parquetV1Table"))
    }

  // Collated cases are Spark 4.0+ only, since the COLLATE syntax does not parse on 3.4 and 3.5.
  // CometLevenshtein reports collated input as Unsupported and CodegenDispatchFallback runs it
  // through the JVM codegen dispatcher, so these measure the dispatcher rather than the native
  // kernel. See https://github.com/apache/datafusion-comet/issues/5591.
  private val collatedStringExpressions =
    if (isSpark40Plus) {
      List(
        StringExprConfig(
          "levenshtein_collated",
          "select levenshtein(c1 collate utf8_lcase, 'test' collate utf8_lcase)" +
            " from parquetV1Table"),
        StringExprConfig(
          "levenshtein_threshold_collated",
          "select levenshtein(c1 collate utf8_lcase, 'test' collate utf8_lcase, 3)" +
            " from parquetV1Table"))
    } else {
      List.empty
    }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val replaceMatchOnly = mainArgs.contains("replace-match-only")
    val replaceColumnOnly = mainArgs.contains("replace-column-only")
    val replaceLongSearchOnly = mainArgs.contains("replace-column-long-only")
    val rows =
      if (replaceMatchOnly || replaceColumnOnly || replaceLongSearchOnly) 65536 else 1024
    runBenchmarkWithTable("String expressions", rows) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // c2 gives expressions that take a length or a count something to vary over per row.
          // `pmod` keeps it non-negative, so `space(c2)` builds a string on every row rather
          // than returning empty for the negative half of the input.
          val longSearchInputs = Seq(16, 256, 1024).flatMap { searchBytes =>
            val flankBytes = (2048 - searchBytes) / 2
            val matchSource =
              s"CONCAT(REPEAT('b', $flankBytes), REPEAT('a', $searchBytes), " +
                s"REPEAT('b', $flankBytes))"
            Seq(
              s"REPEAT('b', 2048) AS replace_long_${searchBytes}_0pct",
              s"CASE WHEN PMOD(value, 10) = 0 THEN $matchSource ELSE " +
                s"REPEAT('b', 2048) END AS replace_long_${searchBytes}_10pct",
              s"$matchSource AS replace_long_${searchBytes}_100pct")
          }
          prepareTable(
            dir,
            spark.sql(
              "SELECT REPEAT(CAST(value AS STRING), 10) AS c1," +
                " CAST(PMOD(value, 200) AS INT) AS c2," +
                " REPEAT('abc---def', 8) AS replace_no_match," +
                " CASE WHEN PMOD(value, 10) = 0 THEN REPEAT('abc123def', 8)" +
                " ELSE REPEAT('abc---def', 8) END AS replace_ten_percent," +
                " REPEAT('abc123def', 8) AS replace_all_match," +
                " CASE WHEN PMOD(value, 20) = 0 THEN CAST(NULL AS STRING)" +
                " WHEN PMOD(value, 3) = 0 THEN ''" +
                " WHEN PMOD(value, 3) = 1 THEN 'ab' ELSE 'XYZ' END AS replace_with" +
                ", " + longSearchInputs.mkString(", ") +
                s" FROM $tbl"))

          val extraConfigs = Map(
            CometConf.getExprAllowIncompatConfigKey("Upper") -> "true",
            CometConf.getExprAllowIncompatConfigKey("Lower") -> "true",
            CometConf.getExprAllowIncompatConfigKey("InitCap") -> "true")

          val cases = if (replaceColumnOnly) {
            stringExpressions.filter(_.name.startsWith("replace_column_match_"))
          } else if (replaceLongSearchOnly) {
            stringExpressions.filter(_.name.startsWith("replace_column_search_"))
          } else if (replaceMatchOnly) {
            stringExpressions.filter(config =>
              config.name.startsWith("replace_match_") ||
                config.name.startsWith("replace_column_match_"))
          } else {
            stringExpressions ++ collatedStringExpressions
          }
          cases.foreach { config =>
            val allConfigs = extraConfigs ++ config.extraCometConfigs
            runBenchmark(config.name) {
              runExpressionBenchmark(config.name, v.toLong, config.query, allConfigs)
            }
          }
        }
      }
    }
  }
}
