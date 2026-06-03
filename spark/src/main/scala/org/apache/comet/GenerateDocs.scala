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

package org.apache.comet

import java.io.{BufferedOutputStream, BufferedReader, FileOutputStream, FileReader}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

import org.apache.comet.CometConf.COMET_ONHEAP_MEMORY_OVERHEAD
import org.apache.comet.ExpressionReference._
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Incompatible, QueryPlanSerde, Unsupported}

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  private val publicConfigs: Set[ConfigEntry[_]] = CometConf.allConfs.filter(_.isPublic).toSet

  /**
   * (expression class simple name, compatible notes, incompatible reasons, unsupported reasons)
   */
  private type CategoryNotes = Seq[(String, Seq[String], Seq[String], Seq[String])]

  /**
   * Mapping from expression category to the compatibility guide page where that category's
   * auto-generated notes should be written, along with a function that produces the notes for
   * that category from the serde maps in `QueryPlanSerde`.
   */
  private def categoryPages: Map[String, (String, () => CategoryNotes)] = Map(
    "array" -> ((
      "compatibility/expressions/array.md",
      () =>
        QueryPlanSerde.arrayExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "datetime" -> ((
      "compatibility/expressions/datetime.md",
      () =>
        QueryPlanSerde.temporalExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "math" -> ((
      "compatibility/expressions/math.md",
      () =>
        QueryPlanSerde.mathExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "struct" -> ((
      "compatibility/expressions/struct.md",
      () =>
        QueryPlanSerde.structExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "aggregate" -> ((
      "compatibility/expressions/aggregate.md",
      () =>
        QueryPlanSerde.aggrSerdeMap.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "string" -> ((
      "compatibility/expressions/string.md",
      () =>
        QueryPlanSerde.stringExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "map" -> ((
      "compatibility/expressions/map.md",
      () =>
        QueryPlanSerde.mapExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "misc" -> ((
      "compatibility/expressions/misc.md",
      () =>
        QueryPlanSerde.miscExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "url" -> ((
      "compatibility/expressions/url.md",
      () =>
        QueryPlanSerde.urlExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })))

  /**
   * Curated status for Spark built-ins that Comet does not serde-support. This list lives in
   * GenerateDocs (not in the serde files) on purpose: it is excluded from the heavy CI path
   * filters (build, spark-sql, iceberg) in dev/ci/compute-changes.py, so editing it (e.g. when an
   * issue is filed) does not trigger those heavy jobs. Keyed by Spark function name.
   */
  private val plannedExpressions: Map[String, PlannedExpr] = Map(
    "aes_decrypt" -> PlannedExpr(
      Planned,
      issue = Some(4558),
      note = Some("Falls back; `StaticInvoke` not allowlisted; planned via codegen dispatch")),
    "aes_encrypt" -> PlannedExpr(
      Planned,
      issue = Some(4558),
      note = Some("Falls back; planned via codegen dispatch; nondeterministic IV by default")),
    "aggregate" -> PlannedExpr(Planned, issue = Some(4224)),
    "any" -> PlannedExpr(Supported),
    "any_value" -> PlannedExpr(Supported),
    "approx_count_distinct" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "approx_percentile" -> PlannedExpr(NotPlanned),
    "approx_top_k" -> PlannedExpr(NotPlanned),
    "approx_top_k_accumulate" -> PlannedExpr(NotPlanned),
    "approx_top_k_combine" -> PlannedExpr(NotPlanned),
    "approx_top_k_estimate" -> PlannedExpr(NotPlanned),
    "array_agg" -> PlannedExpr(
      Planned,
      issue = Some(2524),
      note = Some("Array aggregate (related to `collect_list`)")),
    "array_prepend" -> PlannedExpr(Planned, note = Some("Sibling of `array_append`")),
    "array_size" -> PlannedExpr(Supported),
    "array_sort" -> PlannedExpr(Planned, issue = Some(4224)),
    "assert_true" -> PlannedExpr(
      Planned,
      note = Some("Lowers to `RaiseError`, which falls back")),
    "base64" -> PlannedExpr(
      Planned,
      note = Some("Lowers to `StaticInvoke(encode)` (not allowlisted); falls back")),
    "between" -> PlannedExpr(Supported),
    "bitmap_and_agg" -> PlannedExpr(NotPlanned),
    "bitmap_bit_position" -> PlannedExpr(NotPlanned),
    "bitmap_bucket_number" -> PlannedExpr(NotPlanned),
    "bitmap_construct_agg" -> PlannedExpr(NotPlanned),
    "bitmap_count" -> PlannedExpr(NotPlanned),
    "bitmap_or_agg" -> PlannedExpr(NotPlanned),
    "bool_and" -> PlannedExpr(Supported),
    "bool_or" -> PlannedExpr(Supported),
    "bround" -> PlannedExpr(Planned, issue = Some(4538)),
    "ceil" -> PlannedExpr(Supported, note = Some("Two-arg form falls back")),
    "ceiling" -> PlannedExpr(Supported),
    "collate" -> PlannedExpr(
      Planned,
      issue = Some(2190),
      note = Some("Spark collation (umbrella)")),
    "collation" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded to a literal (Spark 4.0+)")),
    "collect_list" -> PlannedExpr(Planned, issue = Some(2524)),
    "contains" -> PlannedExpr(Supported),
    "conv" -> PlannedExpr(Planned, issue = Some(4538)),
    "count_if" -> PlannedExpr(Supported),
    "count_min_sketch" -> PlannedExpr(NotPlanned),
    "cume_dist" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "curdate" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded to a literal (alias of `current_date`)")),
    "current_catalog" -> PlannedExpr(
      Supported,
      note = Some("Resolved to a literal by the analyzer (`ReplaceCurrentLike`)")),
    "current_database" -> PlannedExpr(
      Supported,
      note = Some("Resolved to a literal by the analyzer (`ReplaceCurrentLike`)")),
    "current_date" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded to a literal before Comet sees the plan")),
    "current_schema" -> PlannedExpr(
      Supported,
      note = Some("Alias of `current_database`; resolved to a literal by the analyzer")),
    "current_time" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Blocked on Spark 4.1 TIME type support")),
    "current_timestamp" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded to a literal before Comet sees the plan")),
    "current_timezone" -> PlannedExpr(Supported),
    "current_user" -> PlannedExpr(
      Supported,
      note = Some("Resolved to a literal by the analyzer; same as `user`")),
    "date_part" -> PlannedExpr(Supported),
    "datepart" -> PlannedExpr(Supported),
    "dayname" -> PlannedExpr(Planned, issue = Some(4544)),
    "decode" -> PlannedExpr(Supported),
    "dense_rank" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "e" -> PlannedExpr(Supported, note = Some("Folds to a literal (like `pi`)")),
    "elt" -> PlannedExpr(Planned, issue = Some(4538)),
    "encode" -> PlannedExpr(
      Planned,
      note = Some("Lowers to `StaticInvoke(encode)` (not allowlisted); falls back")),
    "endswith" -> PlannedExpr(Supported),
    "equal_null" -> PlannedExpr(Supported, note = Some("Lowers to `<=>` (`EqualNullSafe`)")),
    "every" -> PlannedExpr(Supported),
    "exists" -> PlannedExpr(Planned, issue = Some(4224)),
    "explode" -> PlannedExpr(Supported, note = Some("via `CometExplodeExec`")),
    "explode_outer" -> PlannedExpr(
      Supported,
      note = Some("outer=true falls back (Incompatible) " +
        "([audit](../../contributor-guide/expression-audits/generator_funcs.md#explode_outer))")),
    "extract" -> PlannedExpr(Supported),
    "find_in_set" -> PlannedExpr(Planned, issue = Some(4538)),
    "floor" -> PlannedExpr(Supported, note = Some("Two-arg form falls back")),
    "forall" -> PlannedExpr(Planned, issue = Some(4224)),
    "format_number" -> PlannedExpr(Planned, issue = Some(4538)),
    "format_string" -> PlannedExpr(Planned, issue = Some(4538)),
    "from_avro" -> PlannedExpr(NotPlanned),
    "from_protobuf" -> PlannedExpr(NotPlanned),
    "get" -> PlannedExpr(Supported),
    "grouping" -> PlannedExpr(
      Planned,
      note = Some("Grouping indicator for ROLLUP/CUBE/GROUPING SETS")),
    "grouping_id" -> PlannedExpr(
      Planned,
      note = Some("Grouping indicator for ROLLUP/CUBE/GROUPING SETS")),
    "histogram_numeric" -> PlannedExpr(NotPlanned),
    "hll_sketch_agg" -> PlannedExpr(NotPlanned),
    "hll_sketch_estimate" -> PlannedExpr(NotPlanned),
    "hll_union" -> PlannedExpr(NotPlanned),
    "hll_union_agg" -> PlannedExpr(NotPlanned),
    "hour" -> PlannedExpr(Supported),
    "hypot" -> PlannedExpr(Planned, issue = Some(4538)),
    "ifnull" -> PlannedExpr(Supported),
    "ilike" -> PlannedExpr(Supported),
    "inline" -> PlannedExpr(Planned, note = Some("Operator-level generator (like `explode`)")),
    "inline_outer" -> PlannedExpr(
      Planned,
      note = Some("Operator-level generator (like `explode`)")),
    "input_file_block_length" -> PlannedExpr(NotPlanned),
    "input_file_block_start" -> PlannedExpr(NotPlanned),
    "input_file_name" -> PlannedExpr(NotPlanned),
    "is_valid_utf8" -> PlannedExpr(NotPlanned),
    "is_variant_null" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "java_method" -> PlannedExpr(NotPlanned),
    "json_array_length" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "json_object_keys" -> PlannedExpr(Planned, issue = Some(3161)),
    "json_tuple" -> PlannedExpr(Planned, issue = Some(3160)),
    "kll_merge_agg_bigint" -> PlannedExpr(NotPlanned),
    "kll_merge_agg_double" -> PlannedExpr(NotPlanned),
    "kll_merge_agg_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_agg_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_agg_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_agg_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_n_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_n_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_n_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_quantile_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_quantile_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_quantile_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_rank_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_rank_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_get_rank_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_merge_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_merge_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_merge_float" -> PlannedExpr(NotPlanned),
    "kll_sketch_to_string_bigint" -> PlannedExpr(NotPlanned),
    "kll_sketch_to_string_double" -> PlannedExpr(NotPlanned),
    "kll_sketch_to_string_float" -> PlannedExpr(NotPlanned),
    "kurtosis" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "lag" -> PlannedExpr(Supported, note = Some("via `CometWindowExec`")),
    "lead" -> PlannedExpr(Supported, note = Some("via `CometWindowExec`")),
    "levenshtein" -> PlannedExpr(Planned, issue = Some(4538)),
    "listagg" -> PlannedExpr(Planned, note = Some("String aggregation")),
    "localtimestamp" -> PlannedExpr(Supported),
    "locate" -> PlannedExpr(Planned, issue = Some(4538)),
    "log1p" -> PlannedExpr(Planned, issue = Some(4538)),
    "lpad" -> PlannedExpr(Supported),
    "luhn_check" -> PlannedExpr(
      Supported,
      note = Some("Native via `StaticInvoke` (tests: luhn_check.sql)")),
    "make_dt_interval" -> PlannedExpr(Planned, issue = Some(4541)),
    "make_interval" -> PlannedExpr(
      Planned,
      issue = Some(4540),
      note = Some("Produces legacy CalendarInterval; tracked by")),
    "make_time" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Spark 4.1 TIME type; tracked by")),
    "make_timestamp" -> PlannedExpr(Supported),
    "make_timestamp_ltz" -> PlannedExpr(Supported, note = Some("2-arg TIME form falls back")),
    "make_timestamp_ntz" -> PlannedExpr(Supported, note = Some("2-arg TIME form falls back")),
    "make_valid_utf8" -> PlannedExpr(NotPlanned),
    "make_ym_interval" -> PlannedExpr(Planned, issue = Some(4541)),
    "map" -> PlannedExpr(Planned, note = Some("Constructs a map")),
    "map_concat" -> PlannedExpr(Planned, note = Some("Concatenates maps")),
    "map_filter" -> PlannedExpr(Planned, issue = Some(4224)),
    "map_zip_with" -> PlannedExpr(Planned, issue = Some(4224)),
    "mask" -> PlannedExpr(Planned, note = Some("Data masking")),
    "max_by" -> PlannedExpr(Planned, issue = Some(3841)),
    "median" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "min_by" -> PlannedExpr(Planned, issue = Some(3841)),
    "minute" -> PlannedExpr(Supported),
    "mode" -> PlannedExpr(Planned, issue = Some(3970)),
    "monthname" -> PlannedExpr(Planned, issue = Some(4544)),
    "nanvl" -> PlannedExpr(Planned, issue = Some(4538)),
    "now" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded to a literal (alias of `current_timestamp`)")),
    "nth_value" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "ntile" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "nullif" -> PlannedExpr(Supported),
    "nullifzero" -> PlannedExpr(Supported, note = Some("Lowers to `if`/`=` (Spark 4.0+)")),
    "nvl" -> PlannedExpr(Supported),
    "nvl2" -> PlannedExpr(Supported),
    "overlay" -> PlannedExpr(Planned, issue = Some(4538)),
    "parse_json" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "percent_rank" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "percentile" -> PlannedExpr(Planned, issue = Some(4542)),
    "percentile_approx" -> PlannedExpr(NotPlanned),
    "percentile_cont" -> PlannedExpr(Planned, note = Some("Percentile aggregate")),
    "percentile_disc" -> PlannedExpr(Planned, note = Some("Percentile aggregate")),
    "pmod" -> PlannedExpr(Planned, issue = Some(4538)),
    "posexplode" -> PlannedExpr(Supported, note = Some("via `CometExplodeExec`")),
    "posexplode_outer" -> PlannedExpr(
      Supported,
      note = Some(
        "outer=true falls back (Incompatible) " +
          "([audit](../../contributor-guide/expression-audits/" +
          "generator_funcs.md#posexplode_outer))")),
    "position" -> PlannedExpr(Planned, issue = Some(4538)),
    "positive" -> PlannedExpr(Supported),
    "printf" -> PlannedExpr(Planned, issue = Some(4538)),
    "quote" -> PlannedExpr(NotPlanned),
    "raise_error" -> PlannedExpr(Planned, note = Some("Raises a runtime error")),
    "randstr" -> PlannedExpr(Planned, note = Some("Random string (Spark 4.0+)")),
    "rank" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "reduce" -> PlannedExpr(Planned, issue = Some(4224)),
    "reflect" -> PlannedExpr(NotPlanned),
    "regexp_count" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "regexp_extract" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "regexp_extract_all" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "regexp_instr" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "regexp_substr" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "regr_avgx" -> PlannedExpr(
      Supported,
      note = Some(
        "Native: Spark rewrites to `Average` " +
          "(tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551))")),
    "regr_avgy" -> PlannedExpr(
      Supported,
      note = Some(
        "Native: Spark rewrites to `Average` " +
          "(tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551))")),
    "regr_count" -> PlannedExpr(
      Supported,
      note = Some(
        "Native: Spark rewrites to `Count` " +
          "(tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551))")),
    "regr_intercept" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse `covar_pop`/`var_pop` accumulators")),
    "regr_r2" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse the `corr` accumulator")),
    "regr_slope" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse `covar_pop`/`var_pop` accumulators")),
    "regr_sxx" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse `var_pop` accumulator")),
    "regr_sxy" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse `covar_pop` accumulator")),
    "regr_syy" -> PlannedExpr(
      Planned,
      issue = Some(4552),
      note = Some("Falls back; can reuse `var_pop` accumulator")),
    "row_number" -> PlannedExpr(
      Planned,
      issue = Some(2721),
      note = Some("Window function; tracked by")),
    "rpad" -> PlannedExpr(Supported),
    "schema_of_avro" -> PlannedExpr(NotPlanned),
    "schema_of_json" -> PlannedExpr(Planned, issue = Some(3163)),
    "schema_of_variant" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "schema_of_variant_agg" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "second" -> PlannedExpr(Supported),
    "sentences" -> PlannedExpr(NotPlanned),
    "sequence" -> PlannedExpr(Planned, issue = Some(4538)),
    "session_user" -> PlannedExpr(
      Supported,
      note = Some("Alias of `current_user`; resolved to a literal by the analyzer")),
    "session_window" -> PlannedExpr(
      Planned,
      issue = Some(4553),
      note = Some("Time-window grouping; tracked by")),
    "shuffle" -> PlannedExpr(Planned, note = Some("Random array shuffle")),
    "skewness" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "some" -> PlannedExpr(Supported),
    "soundex" -> PlannedExpr(Planned, issue = Some(4538)),
    "split_part" -> PlannedExpr(
      Planned,
      issue = Some(4561),
      note = Some("Lowers to `element_at(StringSplitSQL(...))`; `StringSplitSQL` falls back")),
    "stack" -> PlannedExpr(Planned, note = Some("Operator-level generator")),
    "startswith" -> PlannedExpr(Supported),
    "string_agg" -> PlannedExpr(Planned, note = Some("String aggregation (alias of `listagg`)")),
    "theta_difference" -> PlannedExpr(NotPlanned),
    "theta_intersection" -> PlannedExpr(NotPlanned),
    "theta_intersection_agg" -> PlannedExpr(NotPlanned),
    "theta_sketch_agg" -> PlannedExpr(NotPlanned),
    "theta_sketch_estimate" -> PlannedExpr(NotPlanned),
    "theta_union" -> PlannedExpr(NotPlanned),
    "theta_union_agg" -> PlannedExpr(NotPlanned),
    "time_diff" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Spark 4.1 TIME type; tracked by")),
    "time_trunc" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Spark 4.1 TIME type; tracked by")),
    "to_avro" -> PlannedExpr(NotPlanned),
    "to_binary" -> PlannedExpr(
      Supported,
      note = Some("Hex form accelerated; other formats fall back")),
    "to_char" -> PlannedExpr(Planned, issue = Some(4538)),
    "to_date" -> PlannedExpr(
      Supported,
      note = Some(
        "Rewrites to `Cast` (or `Cast(GetTimestamp)` with a format) before Comet sees the plan")),
    "to_number" -> PlannedExpr(Planned, issue = Some(4538)),
    "to_protobuf" -> PlannedExpr(NotPlanned),
    "to_time" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Spark 4.1 TIME type; tracked by")),
    "to_timestamp" -> PlannedExpr(
      Supported,
      note =
        Some("Rewrites to `Cast` (or `GetTimestamp` with a format) before Comet sees the plan")),
    "to_timestamp_ltz" -> PlannedExpr(
      Supported,
      note = Some("Rewrites to `to_timestamp` (`TimestampType`)")),
    "to_timestamp_ntz" -> PlannedExpr(
      Supported,
      note = Some("Rewrites to `to_timestamp` (`TimestampNTZType`)")),
    "to_varchar" -> PlannedExpr(Planned, issue = Some(4538)),
    "to_variant_object" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "transform" -> PlannedExpr(Planned, issue = Some(4224)),
    "transform_keys" -> PlannedExpr(Planned, issue = Some(4224)),
    "transform_values" -> PlannedExpr(Planned, issue = Some(4224)),
    "try_add" -> PlannedExpr(Supported, note = Some("Datetime/interval form falls back")),
    "try_aes_decrypt" -> PlannedExpr(
      Planned,
      issue = Some(4558),
      note = Some("Falls back; planned via codegen dispatch")),
    "try_avg" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "try_divide" -> PlannedExpr(Supported),
    "try_element_at" -> PlannedExpr(
      Supported,
      note = Some("Lowers to `element_at`; array input (MapType falls back)")),
    "try_make_interval" -> PlannedExpr(
      Planned,
      issue = Some(4540),
      note = Some("Produces legacy CalendarInterval; tracked by")),
    "try_make_timestamp" -> PlannedExpr(
      Supported,
      issue = Some(4554),
      note = Some("Returns a wrong value instead of NULL for invalid inputs")),
    "try_make_timestamp_ltz" -> PlannedExpr(NotPlanned),
    "try_make_timestamp_ntz" -> PlannedExpr(NotPlanned),
    "try_mod" -> PlannedExpr(
      Planned,
      issue = Some(4484),
      note = Some("Lowers to `Remainder` with TRY eval mode, which falls back")),
    "try_multiply" -> PlannedExpr(Supported),
    "try_parse_json" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "try_parse_url" -> PlannedExpr(NotPlanned),
    "try_reflect" -> PlannedExpr(NotPlanned),
    "try_subtract" -> PlannedExpr(Supported),
    "try_sum" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "try_to_binary" -> PlannedExpr(
      Planned,
      note = Some("Lowers to `TryEval(...)`, which falls back")),
    "try_to_date" -> PlannedExpr(
      Planned,
      issue = Some(4556),
      note = Some("Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by")),
    "try_to_number" -> PlannedExpr(Planned, note = Some("TRY variant of `to_number`")),
    "try_to_time" -> PlannedExpr(
      Planned,
      issue = Some(4288),
      note = Some("Spark 4.1 TIME type; tracked by")),
    "try_to_timestamp" -> PlannedExpr(
      Planned,
      issue = Some(4556),
      note = Some("Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by")),
    "try_url_decode" -> PlannedExpr(Supported),
    "try_validate_utf8" -> PlannedExpr(NotPlanned),
    "try_variant_get" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "typeof" -> PlannedExpr(
      Supported,
      note = Some("Foldable; resolved to a literal before Comet sees the plan")),
    "unbase64" -> PlannedExpr(Planned, issue = Some(4538)),
    "uniform" -> PlannedExpr(
      Supported,
      note = Some("Constant-folded; literal arguments only (Spark 4.0+)")),
    "url_decode" -> PlannedExpr(Supported),
    "url_encode" -> PlannedExpr(Supported),
    "user" -> PlannedExpr(
      Supported,
      note = Some("Resolved to a literal by the Spark analyzer before reaching Comet")),
    "uuid" -> PlannedExpr(Planned, note = Some("Nondeterministic random UUID")),
    "validate_utf8" -> PlannedExpr(NotPlanned),
    "variant_get" -> PlannedExpr(Planned, issue = Some(4098), note = Some("tracking")),
    "version" -> PlannedExpr(NotPlanned),
    "width_bucket" -> PlannedExpr(Supported),
    "window" -> PlannedExpr(
      Planned,
      issue = Some(4553),
      note = Some("Time-window grouping; tracked by")),
    "window_time" -> PlannedExpr(
      Planned,
      issue = Some(4553),
      note = Some("Time-window grouping; tracked by")),
    "zeroifnull" -> PlannedExpr(Supported, note = Some("Lowers to `coalesce` (Spark 4.0+)")),
    "zip_with" -> PlannedExpr(Planned, issue = Some(4224)))

  /**
   * Spark function groups rendered as tables, in display order. Families that fall back wholesale
   * (xml_funcs, csv_funcs, geospatial, etc.) are intentionally omitted; they are covered by the
   * "Not currently planned" prose section.
   *
   * Consumed by `generateExpressionReference` (added in the following task).
   */
  private val expressionGroups: Seq[String] = Seq(
    "agg_funcs",
    "array_funcs",
    "bitwise_funcs",
    "collection_funcs",
    "conditional_funcs",
    "conversion_funcs",
    "datetime_funcs",
    "generator_funcs",
    "hash_funcs",
    "json_funcs",
    "lambda_funcs",
    "map_funcs",
    "math_funcs",
    "misc_funcs",
    "predicate_funcs",
    "string_funcs",
    "struct_funcs",
    "url_funcs",
    "variant_funcs",
    "window_funcs")

  /**
   * Map expression class -> compat-guide category, only for categories that have a page. Must
   * stay in sync with `categoryPages`: only categories that have a compat-guide page belong here,
   * so functions in other categories intentionally get no compat link.
   */
  private val classToCategory: Map[Class[_], String] = Seq(
    QueryPlanSerde.arrayExpressions.keys.map((_: Class[_]) -> "array"),
    QueryPlanSerde.temporalExpressions.keys.map((_: Class[_]) -> "datetime"),
    QueryPlanSerde.mathExpressions.keys.map((_: Class[_]) -> "math"),
    QueryPlanSerde.structExpressions.keys.map((_: Class[_]) -> "struct"),
    QueryPlanSerde.stringExpressions.keys.map((_: Class[_]) -> "string"),
    QueryPlanSerde.mapExpressions.keys.map((_: Class[_]) -> "map"),
    QueryPlanSerde.miscExpressions.keys.map((_: Class[_]) -> "misc"),
    QueryPlanSerde.urlExpressions.keys.map((_: Class[_]) -> "url"),
    QueryPlanSerde.aggrSerdeMap.keys.map((_: Class[_]) -> "aggregate")).flatten.toMap

  /** Build the serde-derived doc facts for a function class, if Comet serde-supports it. */
  private def serdeDocInfoFor(className: String): Option[SerdeDocInfo] = {
    // scalastyle:off classforname
    val clsOpt = Try(Class.forName(className)).toOption
    // scalastyle:on classforname
    clsOpt.flatMap { cls =>
      // The cast is erased at runtime; the lookup is by key equality, so a class that is
      // not actually an Expression subtype simply matches no key and yields None.
      val exprSerde = QueryPlanSerde.exprSerdeMap
        .get(cls.asInstanceOf[Class[_ <: Expression]])
      val aggSerde = QueryPlanSerde.aggrSerdeMap.get(cls)
      val notesAndSummary: Option[(Option[String], Boolean)] = exprSerde match {
        case Some(s) =>
          Some(
            (
              s.getExpressionSummary,
              s.getCompatibleNotes().nonEmpty || s.getIncompatibleReasons().nonEmpty ||
                s.getUnsupportedReasons().nonEmpty))
        case None =>
          aggSerde.map { s =>
            (
              s.getExpressionSummary,
              s.getCompatibleNotes().nonEmpty || s.getIncompatibleReasons().nonEmpty ||
                s.getUnsupportedReasons().nonEmpty)
          }
      }
      notesAndSummary.map { case (summary, hasCompat) =>
        // scalastyle:off caselocale
        val anchor = cls.getSimpleName.toLowerCase
        // scalastyle:on caselocale
        SerdeDocInfo(
          summary = summary,
          hasCompatContent = hasCompat,
          category = classToCategory.get(cls),
          anchor = anchor)
      }
    }
  }

  /**
   * Resolve all rows for a group, logging warnings for unclassified builtins.
   *
   * Consumed by `generateExpressionReference` (added in the following task).
   */
  private def rowsForGroup(group: String, entries: Seq[FunctionEntry]): Seq[ReferenceRow] = {
    entries.filter(_.group == group).map { e =>
      val (row, warn) =
        resolveRow(e, serdeDocInfoFor(e.className), plannedExpressions.get(e.name))
      // scalastyle:off println
      warn.foreach(w => println(s"[GenerateDocs][WARN] $w"))
      // scalastyle:on println
      row
    }
  }

  def main(args: Array[String]): Unit = {
    val userGuideLocation = args(0)
    generateConfigReference(s"$userGuideLocation/configs.md")
    generateCompatibilityGuide(s"$userGuideLocation/compatibility/expressions/cast.md")
    for ((category, (page, notesFn)) <- categoryPages) {
      generateExpressionCompatNotes(s"$userGuideLocation/$page", category, notesFn())
    }
    generateExpressionReference(s"$userGuideLocation/expressions.md")
  }

  private def generateExpressionReference(filename: String): Unit = {
    val entries = builtinFunctions()
    val pattern = "<!--BEGIN:EXPR_TABLE\\[(.*)]-->".r
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      line.trim match {
        case pattern(group) =>
          val table = ExpressionReference.renderTable(rowsForGroup(group, entries))
          w.write(s"$table\n".getBytes)
        case _ =>
      }
    }
    w.close()
  }

  private def generateConfigReference(filename: String): Unit = {
    val pattern = "<!--BEGIN:CONFIG_TABLE\\[(.*)]-->".r
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      line match {
        case pattern(category) =>
          w.write("<!-- prettier-ignore-start -->\n".getBytes)
          w.write("| Config | Description | Default Value |\n".getBytes)
          w.write("|--------|-------------|---------------|\n".getBytes)
          category match {
            case "enable_expr" =>
              for (expr <- QueryPlanSerde.exprSerdeMap.keys.map(_.getSimpleName).toList.sorted) {
                val config = s"spark.comet.expression.$expr.enabled"
                w.write(
                  s"| `$config` | Enable Comet acceleration for `$expr` | true |\n".getBytes)
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
            case "enable_agg_expr" =>
              for (expr <- QueryPlanSerde.aggrSerdeMap.keys.map(_.getSimpleName).toList.sorted) {
                val config = s"spark.comet.expression.$expr.enabled"
                w.write(
                  s"| `$config` | Enable Comet acceleration for `$expr` | true |\n".getBytes)
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
            case _ =>
              val urlPattern = """Comet\s+(Compatibility|Tuning|Tracing)\s+Guide\s+\(""".r
              val confs = publicConfigs.filter(_.category == category).toList.sortBy(_.key)
              for (conf <- confs) {
                // convert links to Markdown
                val doc =
                  urlPattern.replaceAllIn(conf.doc.trim, m => s"[Comet ${m.group(1)} Guide](")
                // append env var info if present
                val docWithEnvVar = conf.envVar match {
                  case Some(envVarName) =>
                    s"$doc It can be overridden by the environment variable `$envVarName`."
                  case None => doc
                }
                if (conf.defaultValue.isEmpty) {
                  w.write(s"| `${conf.key}` | $docWithEnvVar | |\n".getBytes)
                } else {
                  val isBytesConf = conf.key == COMET_ONHEAP_MEMORY_OVERHEAD.key
                  if (isBytesConf) {
                    val bytes = conf.defaultValue.get.asInstanceOf[Long]
                    w.write(s"| `${conf.key}` | $docWithEnvVar | $bytes MiB |\n".getBytes)
                  } else {
                    val defaultVal = conf.defaultValueString
                    w.write(s"| `${conf.key}` | $docWithEnvVar | $defaultVal |\n".getBytes)
                  }
                }
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
          }
        case _ =>
      }
    }
    w.close()
  }

  private def generateCompatibilityGuide(filename: String): Unit = {
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == "<!--BEGIN:CAST_LEGACY_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.LEGACY)
      } else if (line.trim == "<!--BEGIN:CAST_TRY_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.TRY)
      } else if (line.trim == "<!--BEGIN:CAST_ANSI_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.ANSI)
      }
    }
    w.close()
  }

  private def generateExpressionCompatNotes(
      filename: String,
      category: String,
      notes: CategoryNotes): Unit = {
    val beginTag = s"<!--BEGIN:EXPR_COMPAT[$category]-->"
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == beginTag) {
        writeExpressionCompatNotes(w, notes)
      }
    }
    w.close()
  }

  private def writeExpressionCompatNotes(w: BufferedOutputStream, notes: CategoryNotes): Unit = {
    val sorted = notes.sortBy(_._1).filter { case (_, compat, incompat, unsupported) =>
      compat.nonEmpty || incompat.nonEmpty || unsupported.nonEmpty
    }
    for ((name, compat, incompat, unsupported) <- sorted) {
      w.write(s"\n## $name\n".getBytes)
      if (compat.nonEmpty) {
        w.write(
          ("\nThe following differences from Spark are always present and do not require" +
            " any additional configuration:\n\n").getBytes)
        for (note <- compat) {
          w.write(s"- $note\n".getBytes)
        }
      }
      if (incompat.nonEmpty) {
        w.write(
          (s"\nThe following incompatibilities cause `$name` to fall back to Spark by default." +
            s" Set `spark.comet.expression.$name.allowIncompatible=true` to enable Comet" +
            " acceleration despite these differences.\n\n").getBytes)
        for (reason <- incompat) {
          w.write(s"- $reason\n".getBytes)
        }
      }
      if (unsupported.nonEmpty) {
        w.write("\nThe following cases are not supported by Comet:\n\n".getBytes)
        for (reason <- unsupported) {
          w.write(s"- $reason\n".getBytes)
        }
      }
    }
  }

  private def writeCastMatrixForMode(w: BufferedOutputStream, mode: CometEvalMode.Value): Unit = {
    val sortedTypes = CometCast.supportedTypes.sortBy(_.typeName)
    val typeNames = sortedTypes.map(_.typeName.replace("(10,2)", ""))

    // Collect annotations for meaningful notes
    val annotations = mutable.ListBuffer[(String, String, String)]()

    w.write("<!-- prettier-ignore-start -->\n".getBytes)

    // Write header row
    w.write("| |".getBytes)
    for (toTypeName <- typeNames) {
      w.write(s" $toTypeName |".getBytes)
    }
    w.write("\n".getBytes)

    // Write separator row
    w.write("|---|".getBytes)
    for (_ <- typeNames) {
      w.write("---|".getBytes)
    }
    w.write("\n".getBytes)

    // Write data rows
    for ((fromType, fromTypeName) <- sortedTypes.zip(typeNames)) {
      w.write(s"| $fromTypeName |".getBytes)
      for ((toType, toTypeName) <- sortedTypes.zip(typeNames)) {
        val cell = if (fromType == toType) {
          "-"
        } else if (!Cast.canCast(fromType, toType)) {
          "N/A"
        } else {
          val supportLevel = CometCast.isSupported(fromType, toType, None, mode)
          supportLevel match {
            case Compatible(notes) =>
              notes.filter(_.trim.nonEmpty).foreach { note =>
                annotations += ((fromTypeName, toTypeName, note.trim.replace("(10,2)", "")))
              }
              "C"
            case Incompatible(notes) =>
              notes.filter(_.trim.nonEmpty).foreach { note =>
                annotations += ((fromTypeName, toTypeName, note.trim.replace("(10,2)", "")))
              }
              "I"
            case Unsupported(_) =>
              "U"
          }
        }
        w.write(s" $cell |".getBytes)
      }
      w.write("\n".getBytes)
    }

    // Write annotations if any
    if (annotations.nonEmpty) {
      w.write("\n**Notes:**\n".getBytes)
      for ((from, to, note) <- annotations.distinct) {
        w.write(s"- **$from -> $to**: $note\n".getBytes)
      }
    }

    w.write("<!-- prettier-ignore-end -->\n".getBytes)
  }

  /** Read file into memory */
  private def readFile(filename: String): Seq[String] = {
    val r = new BufferedReader(new FileReader(filename))
    val buffer = new ListBuffer[String]()
    var line = r.readLine()
    var skipping = false
    while (line != null) {
      if (line.startsWith("<!--BEGIN:")) {
        buffer += line
        skipping = true
      } else if (line.startsWith("<!--END:")) {
        buffer += line
        skipping = false
      } else if (!skipping) {
        buffer += line
      }
      line = r.readLine()
    }
    r.close()
    buffer.toSeq
  }
}
