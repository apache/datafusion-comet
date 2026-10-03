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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.CometTestBase

/**
 * Sweeps Spark's `-0.0` and NaN semantics across operator contexts and the expressions that
 * accept `FLOAT` or `DOUBLE` input (#6385).
 *
 * The edge values are `0.0`, `-0.0`, a canonical NaN and a NaN with the sign bit set, plus `1.0`,
 * `-1.0`, both infinities and `NULL`. A Parquet round trip through Spark's writer canonicalizes
 * NaN, so the sign-bit NaN is made at query time by negating a stored NaN. That works on every
 * platform; on x86-64, every NaN that arithmetic produces also has the sign bit set.
 *
 * A case listed in [[CometFloatSemanticsSuite.knownGaps]] is expected to return a different
 * answer from Spark. The case fails once it matches, so that a fix, or a DataFusion upgrade that
 * changes float behavior, removes its entry from the list instead of leaving it stale.
 */
class CometFloatSemanticsSuite extends CometTestBase {

  import CometFloatSemanticsSuite._

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE TABLE fs_edge_raw(id INT, d DOUBLE, f FLOAT, neg BOOLEAN) USING parquet")
    sql(s"INSERT INTO fs_edge_raw VALUES ${edgeRows.mkString(", ")}")
    sql("""CREATE TABLE fs_pair_raw USING parquet AS
        |SELECT a.id * 10 + b.id AS id, a.d, a.f, a.neg AS dneg, b.d AS e, b.f AS h, b.neg AS eneg
        |FROM fs_edge_raw a CROSS JOIN fs_edge_raw b""".stripMargin)
    // One row per edge value, in columns d (DOUBLE) and f (FLOAT).
    sql("""CREATE TEMPORARY VIEW fs_e AS
        |SELECT id, IF(neg, -d, d) AS d, IF(neg, -f, f) AS f FROM fs_edge_raw""".stripMargin)
    // Every ordered pair of edge values, in columns (d, e) and (f, h).
    sql("""CREATE TEMPORARY VIEW fs_p AS
        |SELECT id, IF(dneg, -d, d) AS d, IF(eneg, -e, e) AS e,
        |  IF(dneg, -f, f) AS f, IF(eneg, -h, h) AS h
        |FROM fs_pair_raw""".stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP VIEW IF EXISTS fs_e")
      sql("DROP VIEW IF EXISTS fs_p")
      sql("DROP TABLE IF EXISTS fs_edge_raw")
      sql("DROP TABLE IF EXISTS fs_pair_raw")
    } finally {
      super.afterAll()
    }
  }

  for (c <- cases) {
    test(c.name) {
      knownGaps.find(_.covers(c)) match {
        case None => checkSparkAnswer(c.sql)
        case Some(gap) =>
          val matched =
            try {
              checkSparkAnswer(c.sql)
              true
            } catch {
              case e: TestFailedException if e.getMessage.startsWith("Results do not match") =>
                false
            }
          assert(
            !matched,
            s"This case now matches Spark. Remove it from the known gaps for ${gap.issue}.")
      }
    }
  }
}

object CometFloatSemanticsSuite {

  private val edgeRows = Seq(
    "(1, 0.0D, float(0.0), false)",
    "(2, double('-0.0'), float('-0.0'), false)",
    "(3, double('NaN'), float('NaN'), false)",
    "(4, double('NaN'), float('NaN'), true)",
    "(5, 1.0D, float(1.0), false)",
    "(6, -1.0D, float(-1.0), false)",
    "(7, double('Infinity'), float('Infinity'), false)",
    "(8, double('-Infinity'), float('-Infinity'), false)",
    "(9, NULL, NULL, false)")

  /**
   * Column names for one float type. `c` is the column of `fs_e`, and `l` and `r` are the two
   * columns of `fs_p`.
   */
  case class FloatType(name: String, sqlType: String, c: String, l: String, r: String) {
    def lit(v: String): String = s"CAST('$v' AS $sqlType)"

    /** Literals for the edge values, including a constant-folded sign-bit NaN. */
    def literals: Seq[String] =
      Seq(lit("0.0"), lit("-0.0"), lit("NaN"), s"-${lit("NaN")}", lit("1.0"))
  }

  val types: Seq[FloatType] = Seq(
    FloatType("double", "DOUBLE", c = "d", l = "d", r = "e"),
    FloatType("float", "FLOAT", c = "f", l = "f", r = "h"))

  case class Case(group: String, context: String, variant: String, tpe: FloatType, sql: String) {
    def name: String =
      s"$group: $context${if (variant.isEmpty) "" else s", $variant"} [${tpe.name}]"
  }

  case class KnownGap(issue: String, reason: String, covers: Case => Boolean)

  private def render(template: String, t: FloatType): String =
    template
      .replace("{c}", t.c)
      .replace("{l}", t.l)
      .replace("{r}", t.r)
      .replace("{T}", t.sqlType)

  val comparisonOps: Seq[String] = Seq("=", "<=>", "!=", "<", "<=", ">", ">=")

  /**
   * Operator contexts that host a comparison of two columns, `{l} {op} {r}` over `fs_p`, or of
   * one column with itself over `fs_e` for the nested loop join.
   */
  val comparisonContexts: Seq[(String, String)] = Seq(
    "Project" -> "SELECT id, {l} {op} {r} FROM fs_p",
    "Filter" -> "SELECT id FROM fs_p WHERE {l} {op} {r}",
    "aggregate argument" -> "SELECT id, max(IF({l} {op} {r}, 1, 0)) FROM fs_p GROUP BY id",
    "aggregate FILTER" ->
      "SELECT id, count(*) FILTER (WHERE {l} {op} {r}) FROM fs_p GROUP BY id",
    "broadcast hash join condition" ->
      "SELECT /*+ BROADCAST(b) */ a.id FROM fs_p a JOIN fs_p b ON a.id = b.id AND a.{l} {op} b.{r}",
    "shuffled hash join condition" ->
      ("SELECT /*+ SHUFFLE_HASH(b) */ a.id FROM fs_p a JOIN fs_p b " +
        "ON a.id = b.id AND a.{l} {op} b.{r}"),
    "sort merge join condition" ->
      "SELECT /*+ MERGE(b) */ a.id FROM fs_p a JOIN fs_p b ON a.id = b.id AND a.{l} {op} b.{r}",
    "nested loop join condition" ->
      "SELECT /*+ BROADCAST(b) */ a.id, b.id FROM fs_e a JOIN fs_e b ON a.{c} {op} b.{c}",
    "sort key" -> "SELECT id FROM fs_p ORDER BY {l} {op} {r}, id",
    "Generate" -> "SELECT id, x FROM fs_p LATERAL VIEW explode(array({l} {op} {r})) t AS x",
    "window function argument" ->
      "SELECT id, max(IF({l} {op} {r}, 1, 0)) OVER (PARTITION BY id % 7) FROM fs_p",
    "window partition key" -> "SELECT id, count(*) OVER (PARTITION BY {l} {op} {r}) FROM fs_p",
    "array operands, Project" -> "SELECT id, array({l}) {op} array({r}) FROM fs_p",
    "array operands, aggregate argument" ->
      "SELECT id, max(IF(array({l}) {op} array({r}), 1, 0)) FROM fs_p GROUP BY id",
    "struct operands, Project" ->
      "SELECT id, named_struct('x', {l}) {op} named_struct('x', {r}) FROM fs_p",
    "struct operands, aggregate argument" ->
      ("SELECT id, max(IF(named_struct('x', {l}) {op} named_struct('x', {r}), 1, 0)) " +
        "FROM fs_p GROUP BY id"))

  private def comparisonCases: Seq[Case] =
    for {
      t <- types
      (context, template) <- comparisonContexts
      op <- comparisonOps
    } yield Case("comparison", context, op, t, render(template.replace("{op}", op), t))

  /** A column compared with each edge literal, on either side. */
  private def literalCases: Seq[Case] =
    for {
      t <- types
      op <- comparisonOps
      (context, from, where) <- Seq(
        ("Project", "fs_e", false),
        ("Filter", "fs_e", true),
        // A stored column compared with a literal is pushed into the Parquet reader.
        ("scan filter", "fs_edge_raw", true))
    } yield {
      // One UNION ALL branch per predicate. With all ten `=` predicates in one projection,
      // Spark 4.1.3's whole-stage codegen returns false for `0.0 = 0.0`, and is right with
      // subexpression elimination turned off.
      val preds = t.literals.flatMap(lit => Seq(s"{c} $op $lit", s"$lit $op {c}"))
      val sql = preds.zipWithIndex
        .map { case (p, i) =>
          if (where) s"SELECT id, $i FROM $from WHERE $p" else s"SELECT id, $i, $p FROM $from"
        }
        .mkString(" UNION ALL ")
      Case("literal comparison", context, op, t, render(sql, t))
    }

  /** Contexts where Spark normalizes a float key, or compares it with SQL ordering. */
  val keyContexts: Seq[(String, String)] = Seq(
    "GROUP BY" -> "SELECT {c}, count(*) FROM fs_e GROUP BY {c}",
    "DISTINCT" -> "SELECT DISTINCT {c} FROM fs_e",
    "count(DISTINCT)" -> "SELECT count(DISTINCT {c}) FROM fs_e",
    "broadcast hash join key" ->
      "SELECT /*+ BROADCAST(b) */ a.id, b.id FROM fs_e a JOIN fs_e b ON a.{c} = b.{c}",
    "shuffled hash join key" ->
      "SELECT /*+ SHUFFLE_HASH(b) */ a.id, b.id FROM fs_e a JOIN fs_e b ON a.{c} = b.{c}",
    "sort merge join key" ->
      "SELECT /*+ MERGE(b) */ a.id, b.id FROM fs_e a JOIN fs_e b ON a.{c} = b.{c}",
    "null-safe join key" -> "SELECT a.id, b.id FROM fs_e a JOIN fs_e b ON a.{c} <=> b.{c}",
    "left semi join key" ->
      ("SELECT a.id FROM fs_e a LEFT SEMI JOIN (SELECT * FROM fs_e WHERE id IN (2, 4)) b " +
        "ON a.{c} = b.{c}"),
    "left anti join key" ->
      ("SELECT a.id FROM fs_e a LEFT ANTI JOIN (SELECT * FROM fs_e WHERE id IN (2, 4)) b " +
        "ON a.{c} = b.{c}"),
    "INTERSECT" -> "SELECT {c} FROM fs_e INTERSECT SELECT {c} FROM fs_e WHERE id IN (2, 4)",
    "EXCEPT" -> "SELECT {c} FROM fs_e EXCEPT SELECT {c} FROM fs_e WHERE id IN (2, 4)",
    "IN list" ->
      "SELECT id FROM fs_e WHERE {c} IN (CAST('0.0' AS {T}), CAST('NaN' AS {T}))",
    "IN subquery" -> "SELECT id FROM fs_e WHERE {c} IN (SELECT {c} FROM fs_e WHERE id IN (2, 4))",
    "ORDER BY" -> "SELECT id FROM fs_e ORDER BY {c}, id",
    "ORDER BY DESC" -> "SELECT id FROM fs_e ORDER BY {c} DESC NULLS LAST, id",
    "window partition key" -> "SELECT id, count(*) OVER (PARTITION BY {c}) FROM fs_e",
    "window order key, rank" -> "SELECT id, rank() OVER (ORDER BY {c}) FROM fs_e",
    "window order key, dense_rank" -> "SELECT id, dense_rank() OVER (ORDER BY {c}) FROM fs_e",
    "hash repartition" ->
      "SELECT id, spark_partition_id() FROM (SELECT /*+ REPARTITION(4, {c}) */ * FROM fs_e)",
    "nested GROUP BY" -> "SELECT array({c}), count(*) FROM fs_e GROUP BY array({c})",
    "nested ORDER BY" -> "SELECT id FROM fs_e ORDER BY array({c}), id",
    "nested window order key" ->
      "SELECT id, rank() OVER (ORDER BY named_struct('x', {c})) FROM fs_e")

  private def keyCases: Seq[Case] =
    for {
      t <- types
      (context, template) <- keyContexts
    } yield Case("key", context, "", t, render(template, t))

  /** Expressions with float input, over one column of `fs_e` or two columns of `fs_p`. */
  val expressions: Seq[(String, String)] = Seq(
    "abs" -> "SELECT id, abs({c}) FROM fs_e",
    "negative" -> "SELECT id, -{c} FROM fs_e",
    "signum" -> "SELECT id, signum({c}) FROM fs_e",
    "ceil" -> "SELECT id, ceil({c}) FROM fs_e",
    "floor" -> "SELECT id, floor({c}) FROM fs_e",
    "round" -> "SELECT id, round({c}, 1), bround({c}, 1) FROM fs_e",
    "sqrt" -> "SELECT id, sqrt({c}) FROM fs_e",
    "isnan" -> "SELECT id, isnan({c}) FROM fs_e",
    "cast to string" -> "SELECT id, CAST({c} AS STRING) FROM fs_e",
    "cast to integral" -> "SELECT id, try_cast({c} AS INT), try_cast({c} AS BIGINT) FROM fs_e",
    "cast to decimal" -> "SELECT id, CAST({c} AS DECIMAL(10, 2)) FROM fs_e",
    "cast to boolean" -> "SELECT id, CAST({c} AS BOOLEAN) FROM fs_e",
    "cast between float types" -> "SELECT id, CAST({c} AS DOUBLE), CAST({c} AS FLOAT) FROM fs_e",
    "cast array to string" -> "SELECT id, CAST(array({c}) AS STRING) FROM fs_e",
    "hash" -> "SELECT id, hash({c}) FROM fs_e",
    "xxhash64" -> "SELECT id, xxhash64({c}) FROM fs_e",
    "hash of array" -> "SELECT id, hash(array({c})), xxhash64(array({c})) FROM fs_e",
    "hash of struct" ->
      "SELECT id, hash(named_struct('x', {c})), xxhash64(named_struct('x', {c})) FROM fs_e",
    "greatest" -> "SELECT id, greatest({l}, {r}) FROM fs_p",
    "least" -> "SELECT id, least({l}, {r}) FROM fs_p",
    "nanvl" -> "SELECT id, nanvl({l}, {r}) FROM fs_p",
    "IN with column list" -> "SELECT id, {l} IN ({r}, CAST('2.0' AS {T})) FROM fs_p",
    "CASE value WHEN" -> "SELECT id, CASE {l} WHEN {r} THEN 1 ELSE 0 END FROM fs_p",
    "array_contains" -> "SELECT id, array_contains(array({l}, 2), {r}) FROM fs_p",
    "array_position" -> "SELECT id, array_position(array(2, {l}), {r}) FROM fs_p",
    "array_remove" -> "SELECT id, array_remove(array({l}, 2), {r}) FROM fs_p",
    "arrays_overlap" -> "SELECT id, arrays_overlap(array({l}), array({r})) FROM fs_p",
    "array_distinct" -> "SELECT id, array_distinct(array({l}, {r})) FROM fs_p",
    "array_union" -> "SELECT id, array_union(array({l}), array({r})) FROM fs_p",
    "array_intersect" -> "SELECT id, array_intersect(array({l}), array({r})) FROM fs_p",
    "array_except" -> "SELECT id, array_except(array({l}), array({r})) FROM fs_p",
    "sort_array" -> "SELECT id, sort_array(array({l}, {r}, 2)) FROM fs_p",
    "sort_array descending" -> "SELECT id, sort_array(array({l}, {r}, 2), false) FROM fs_p",
    "array_sort" -> "SELECT id, array_sort(array({l}, {r}, 2)) FROM fs_p",
    "array_max" -> "SELECT id, array_max(array({l}, {r})) FROM fs_p",
    "array_min" -> "SELECT id, array_min(array({l}, {r})) FROM fs_p",
    "map lookup" ->
      "SELECT id, map({l}, 1)[{r}], element_at(map({l}, 1), {r}) FROM fs_p WHERE {l} IS NOT NULL",
    "map_contains_key" ->
      "SELECT id, map_contains_key(map({l}, 1), {r}) FROM fs_p WHERE {l} IS NOT NULL")

  private def expressionCases: Seq[Case] =
    for {
      t <- types
      (name, template) <- expressions
    } yield Case("expression", name, "", t, render(template, t))

  /** Each ordered pair of edge values as a group of two rows, with the position of each. */
  private val pairGroups =
    "(SELECT id, pos, x FROM fs_p LATERAL VIEW posexplode(array({l}, {r})) t AS pos, x)"

  /** Each pair as (l, r, r), so the mode is unique under every Spark equality rule. */
  private val tripleGroups =
    "(SELECT id, x FROM fs_p LATERAL VIEW explode(array({l}, {r}, {r})) t AS x)"

  val aggregates: Seq[(String, String)] = Seq(
    "max" -> s"SELECT id, max(x) FROM $pairGroups GROUP BY id",
    "min" -> s"SELECT id, min(x) FROM $pairGroups GROUP BY id",
    "max_by" -> s"SELECT id, max_by(pos, x) FROM $pairGroups GROUP BY id",
    "min_by" -> s"SELECT id, min_by(pos, x) FROM $pairGroups GROUP BY id",
    "sum" -> s"SELECT id, sum(x) FROM $pairGroups GROUP BY id",
    "avg" -> s"SELECT id, avg(x) FROM $pairGroups GROUP BY id",
    "count(DISTINCT)" -> s"SELECT id, count(DISTINCT x) FROM $pairGroups GROUP BY id",
    "approx_count_distinct" ->
      s"SELECT id, approx_count_distinct(x) FROM $pairGroups GROUP BY id",
    "collect_set" -> s"SELECT id, size(collect_set(x)) FROM $pairGroups GROUP BY id",
    "mode" -> s"SELECT id, mode(x) FROM $tripleGroups GROUP BY id",
    "global max and min" -> "SELECT max({c}), min({c}) FROM fs_e",
    "global max_by and min_by" -> "SELECT max_by(id, {c}), min_by(id, {c}) FROM fs_e",
    "window max and min" ->
      ("SELECT id, pos, max(x) OVER (PARTITION BY id), min(x) OVER (PARTITION BY id) " +
        s"FROM $pairGroups"))

  private def aggregateCases: Seq[Case] =
    for {
      t <- types
      (name, template) <- aggregates
    } yield Case("aggregate", name, "", t, render(template, t))

  val cases: Seq[Case] =
    comparisonCases ++ literalCases ++ keyCases ++ expressionCases ++ aggregateCases

  private def issue(n: Int): String = s"https://github.com/apache/datafusion-comet/issues/$n"

  private def in(group: String, contexts: String*)(c: Case): Boolean =
    c.group == group && contexts.contains(c.context)

  val knownGaps: Seq[KnownGap] = Seq(
    KnownGap(
      issue(6385),
      "A comparison outside Project and Filter compares raw Arrow values, where a sign-bit NaN " +
        "sorts below every other value. Equi-join conditions already match.",
      c =>
        in("comparison", "aggregate argument", "aggregate FILTER", "sort key", "Generate")(c) ||
          (c.group == "comparison" && c.context.endsWith("join condition") &&
            !Set("=", "<=>").contains(c.variant))),
    KnownGap(
      issue(6385),
      "A data filter pushed into the Parquet reader compares a stored column with a sign-bit " +
        "NaN literal raw, and drops rows that Spark keeps.",
      c => in("literal comparison", "scan filter")(c) && c.variant != "!="),
    KnownGap(
      issue(6157),
      "Ordering and null-safe comparisons of arrays and structs compare float leaves raw.",
      c =>
        c.group == "comparison" &&
          (c.context.startsWith("array operands") || c.context.startsWith("struct operands")) &&
          !Set("=", "!=").contains(c.variant)),
    KnownGap(
      issue(6385),
      "min, max, greatest and least order floats by IEEE 754 total order.",
      c =>
        in("expression", "greatest", "least")(c) ||
          in("aggregate", "max", "min", "global max and min", "window max and min")(c)),
    KnownGap(
      issue(5312),
      "collect_set before Spark 4.2 treats -0.0 and 0.0 as one value and NaNs as distinct.",
      c => in("aggregate", "collect_set")(c) && !CometSparkSessionExtensions.isSpark42Plus),
    KnownGap(
      issue(6522),
      "signum(-0.0) returns 0.0, where Spark returns -0.0.",
      in("expression", "signum")))
}
