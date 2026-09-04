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

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Expression, JsonToStructs, RuntimeReplaceable, Sequence, StringToMap}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.NullType

import org.apache.comet.serde.{QueryPlanSerde, SupportLevel}

/**
 * Cross-product sweep of the `NullType` shapes the JVM codegen dispatcher admits against the
 * expressions that consume them.
 *
 * Three sweeps share one driver, `sweep`: producers under consumers, producers under operators,
 * and producers nested in a container and then put under a serializing operator.
 */
class CometNullTypeCompositionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * Non-foldable `NullType`-bearing expressions over a live column. Each is a shape that the
   * widened gate lets reach native execution; the constant-folded forms are literals and take a
   * different path, so every producer here references `id`.
   */
  private val arrayOfNull = Seq(
    "transform(array(id), x -> NULL)",
    "filter(array(CAST(NULL AS int)), x -> id IS NOT NULL)")

  private val mapWithNullValue = Seq("map(id, NULL)")

  private val mapWithNullKey = Seq("transform_values(map(), (k, v) -> id)")

  private val arrayOfStructWithNull = Seq("map_entries(map(id, NULL))")

  private val structWithNull = Seq("named_struct('a', id, 'b', NULL)")

  private val scalarNull = Seq("aggregate(array(id), NULL, (acc, x) -> NULL)")

  /** Consumers valid for a value of any type, written as templates over the producer `%s`. */
  private val anyTypeConsumers = Seq(
    "to_json(struct(%s AS c))",
    "to_csv(struct(%s AS c))",
    "hash(%s)",
    "xxhash64(%s)",
    "CAST(%s AS string)",
    "CASE WHEN id > 2 THEN %s END",
    "IF(id > 2, %s, NULL)",
    "coalesce(%s, %s)",
    "%s IS NULL",
    "%s = %s",
    "%s <=> %s")

  /** Consumers valid for any `array<T>`. */
  private val arrayConsumers = Seq(
    "size(%s)",
    "reverse(%s)",
    "array_distinct(%s)",
    "sort_array(%s)",
    "array_sort(%s)",
    "element_at(%s, 1)",
    "slice(%s, 1, 1)",
    "array_repeat(%s, 2)",
    "array_union(%s, %s)",
    "array_except(%s, %s)",
    "array_intersect(%s, %s)",
    // Set ops only diverge when a Null-typed side meets a side that actually holds entries, so
    // the same-producer templates above cannot reach that branch on their own.
    "array_union(%s, array(1))",
    "array_union(array(1), %s)",
    "array_except(%s, array(1))",
    "array_intersect(%s, array(1))",
    "arrays_overlap(%s, %s)",
    "arrays_zip(%s, %s)",
    "concat(%s, %s)",
    "flatten(array(%s))",
    "array_position(%s, NULL)",
    "array_contains(%s, NULL)",
    // Spark rejects a NullType needle, so the typed needle is what reaches the serde.
    "array_contains(%s, %s[0])",
    "array_remove(%s, NULL)",
    "array_append(%s, NULL)",
    "array_insert(%s, 1, NULL)",
    "exists(%s, x -> x IS NULL)",
    "forall(%s, x -> x IS NULL)",
    "filter(%s, x -> x IS NULL)",
    "transform(%s, x -> x)",
    "aggregate(%s, 0, (acc, x) -> acc)",
    "zip_with(%s, %s, (x, y) -> x)",
    "map_from_entries(arrays_zip(%s, %s))",
    "%s[0]",
    "array_compact(%s)",
    "array_max(%s)",
    "array_min(%s)",
    "array_join(%s, ',')",
    // shuffle is non-deterministic, so only the sorted result compares.
    "sort_array(shuffle(%s))",
    "map_from_arrays(%s, array(id))") ++ anyTypeConsumers

  /** Consumers valid for any `map<K, V>`. */
  private val mapConsumers = Seq(
    "size(%s)",
    "map_keys(%s)",
    "map_values(%s)",
    "map_entries(%s)",
    "map_concat(%s, %s)",
    "map_filter(%s, (k, v) -> k IS NOT NULL)",
    "transform_values(%s, (k, v) -> v)",
    "transform_keys(%s, (k, v) -> k)",
    "map_zip_with(%s, %s, (k, v1, v2) -> v1)",
    "map_from_entries(map_entries(%s))",
    "element_at(%s, id)",
    "%s[id]") ++ anyTypeConsumers

  private val structConsumers = Seq(
    "%s.a",
    "%s.b",
    "struct(%s)",
    "array(%s)",
    "array(%s).b",
    "size(array(%s))") ++ anyTypeConsumers

  private val scalarConsumers = Seq(
    "array(%s)",
    "map(id, %s)",
    "named_struct('a', %s)",
    "coalesce(%s, NULL)",
    "abs(%s)",
    // Spark's analyzer rejects a void scalar in an array position (no implicit cast), so these
    // stay skipped; they are here so the sweep notices if that ever changes.
    "array_union(%s, array(1))",
    "array_distinct(%s)",
    "array_contains(%s, 1)",
    "CAST(%s AS array<int>)") ++ anyTypeConsumers

  private def cases(wrap: String => String = identity): Seq[(String, String)] =
    Seq(
      arrayOfNull -> arrayConsumers,
      arrayOfStructWithNull -> arrayConsumers,
      mapWithNullValue -> mapConsumers,
      mapWithNullKey -> mapConsumers,
      structWithNull -> structConsumers,
      scalarNull -> scalarConsumers).flatMap { case (producers, consumers) =>
      for (p <- producers; c <- consumers)
        yield {
          val wrapped = wrap(p)
          (wrapped, substitute(c, wrapped, p))
        }
    }

  /**
   * Fills a consumer template, putting `first` in the leading placeholder and `rest` in any
   * others. Wrapping only the first argument keeps a stateful producer to a single occurrence:
   * two of them desynchronize inside Spark itself, whereas the first child is one that every
   * consumer evaluates, so a divergence there belongs to Comet.
   */
  private def substitute(template: String, first: String, rest: String): String = {
    val at = template.indexOf("%s")
    template.substring(0, at) + first + template.substring(at + 2).replace("%s", rest)
  }

  /**
   * Makes a producer nullable and non-deterministic, the only input that can tell apart the two
   * copies a serde null guard serializes (see `NullGuard.doubleEvaluationReason`).
   *
   * Applied to the first argument only, so multi-argument consumers stay in the sweep: a serde
   * that null-guards every child, as `CometArraysZip` does, needs just one stateful argument to
   * diverge, and restricting the sweep to single-placeholder consumers hid exactly that case.
   */
  private def nullableNondeterministic(producer: String): String =
    s"IF(monotonically_increasing_id() % 2 = 0, $producer, NULL)"

  /** Makes a producer nullable while keeping it deterministic. */
  private def nullableDeterministic(producer: String): String =
    s"IF(id % 2 = 0, $producer, NULL)"

  /**
   * Non-nullable, non-deterministic, NullType-bearing producers that record the counter, either
   * in their value or, for the `filter` one, in their length. Under a null guard whose other
   * argument is nullable, the THEN branch evaluates them on the guard's filtered rows only, so
   * the counter sequence differs from Spark's even though the stateful child itself is never
   * null; and a lambda producer runs through the JVM codegen dispatcher, whose kernel cache makes
   * the guard's two copies share one counter, so it diverges under a single-argument guard too
   * (`size(%s)` on a head that guarded non-nullable children). Paired with a deterministic
   * producer of the same type for the sibling slot.
   */
  private val statefulProducers: Seq[(String, String, Seq[String])] = Seq(
    (
      "transform(array(id), x -> named_struct('i', monotonically_increasing_id(), 'n', NULL))",
      "transform(array(id), x -> named_struct('i', x, 'n', NULL))",
      arrayConsumers),
    (
      "filter(transform(array(id, 1, 2), x -> named_struct('i', x, 'n', NULL)), " +
        "s -> s.i < monotonically_increasing_id())",
      "filter(transform(array(id, 1, 2), x -> named_struct('i', x, 'n', NULL)), s -> s.i < id)",
      arrayConsumers),
    ("map(monotonically_increasing_id(), NULL)", "map(id, NULL)", mapConsumers),
    (
      "named_struct('i', monotonically_increasing_id(), 'n', NULL)",
      "named_struct('i', id, 'n', NULL)",
      structConsumers))

  /**
   * Each consumer with the stateful producer in the first slot and either a nullable
   * deterministic sibling or a plain one in the others. The stateful producer stays first because
   * Spark's `BinaryExpression.eval` returns NULL on a null left operand without evaluating the
   * right one, so a stateful right operand's counter is an artifact of Spark's short-circuit and
   * not a contract Comet can match; the left operand is evaluated on every row by both engines.
   */
  private def crossInputCases: Seq[(String, String)] =
    for {
      (stateful, deterministic, consumers) <- statefulProducers
      c <- consumers
      rest <- Seq(nullableDeterministic(deterministic), deterministic)
    } yield (stateful, substitute(c, stateful, rest))

  /**
   * Query templates that put a producer under a different physical operator. The consumer sweep
   * holds the operator fixed at a projection, so it never sees the paths that serialize and
   * re-read a value rather than compute over it.
   */
  private val operatorTemplates = Seq(
    "project" -> "SELECT %s AS c FROM t",
    "filter" -> "SELECT %s AS c FROM t WHERE id > 2",
    "sort-by-id" -> "SELECT %s AS c FROM t ORDER BY id",
    "sort-by-value" -> "SELECT %s AS c FROM t ORDER BY c",
    "limit" -> "SELECT %s AS c FROM t LIMIT 3",
    "take-ordered" -> "SELECT %s AS c FROM t ORDER BY id LIMIT 3",
    "groupby-key" -> "SELECT %s AS c, count(*) FROM t GROUP BY c",
    "groupby-value" -> "SELECT id, first(%s) AS c FROM t GROUP BY id",
    "groupby-last" -> "SELECT id, last(%s) AS c FROM t GROUP BY id",
    "collect-list" -> "SELECT collect_list(%s) AS c FROM t",
    "collect-set" -> "SELECT collect_set(%s) AS c FROM t",
    "max" -> "SELECT max(%s) AS c FROM t",
    "min" -> "SELECT min(%s) AS c FROM t",
    "count-distinct" -> "SELECT count(DISTINCT %s) AS c FROM t",
    "distinct" -> "SELECT DISTINCT %s AS c FROM t",
    "union-all" -> "SELECT %s AS c FROM t UNION ALL SELECT %s AS c FROM t",
    "union-distinct" -> "SELECT %s AS c FROM t UNION SELECT %s AS c FROM t",
    "window-order" -> "SELECT %s AS c, row_number() OVER (ORDER BY id) AS r FROM t",
    "window-partition" -> "SELECT id, count(*) OVER (PARTITION BY %s) AS n FROM t",
    // The producer is computed on the build side, so the value itself crosses the exchange
    // (shuffle or broadcast) rather than being projected after the join.
    "join-shuffle" ->
      "SELECT a.id, b.c FROM t a JOIN (SELECT id AS bid, %s AS c FROM t) b ON a.id = b.bid",
    "join-broadcast" ->
      ("SELECT /*+ BROADCAST(b) */ a.id, b.c FROM t a " +
        "JOIN (SELECT id AS bid, %s AS c FROM t) b ON a.id = b.bid"),
    "join-nested-loop" ->
      ("SELECT /*+ BROADCAST(b) */ a.id, b.c FROM t a " +
        "JOIN (SELECT id AS bid, %s AS c FROM t) b ON a.id > b.bid"),
    "expand-cube" -> "SELECT id, count(%s) AS n FROM t GROUP BY CUBE(id)",
    "explode" -> "SELECT explode(%s) AS c FROM t",
    "repartition" -> "SELECT /*+ REPARTITION(3) */ %s AS c FROM t",
    // Above `spark.shuffle.sort.bypassMergeThreshold`, so the JVM shuffle takes its sort-based
    // writer, which hands a whole destination partition to one native call.
    "repartition-many" -> "SELECT /*+ REPARTITION(300) */ %s AS c FROM t",
    "coalesce-partitions" -> "SELECT /*+ COALESCE(1) */ %s AS c FROM t",
    // The value itself is the partitioning key, the join key and a scalar subquery result, so
    // the hash partitioner, the join's key comparison and the subquery's scalar conversion see
    // a NullType rather than only carrying one past.
    "distribute-by-value" -> "SELECT %s AS c FROM t DISTRIBUTE BY c",
    "join-on-value" ->
      ("SELECT a.id, b.id FROM (SELECT id, %s AS c FROM t) a " +
        "JOIN (SELECT id, %s AS c FROM t) b ON a.c <=> b.c AND a.id = b.id"),
    "scalar-subquery" -> "SELECT id, (SELECT max(x) FROM (SELECT %s AS x FROM t)) AS c FROM t",
    "subquery" -> "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE %s IS NOT NULL)",
    "nested-project" -> "SELECT c FROM (SELECT %s AS c, id FROM t ORDER BY id) x")

  /**
   * The subset of `operatorTemplates` that serializes the value rather than only computing over
   * it.
   */
  private val serializingOperators = {
    val names = Set(
      "project",
      "sort-by-id",
      "groupby-value",
      "collect-list",
      "collect-set",
      "repartition",
      "repartition-many",
      "distribute-by-value",
      "join-on-value",
      "union-all",
      "join-shuffle")
    require(names.subsetOf(operatorTemplates.map(_._1).toSet), "unknown operator name")
    operatorTemplates.filter { case (name, _) => names(name) }
  }

  private val arrayOfStructWithNullField = Seq("array(named_struct('a', id, 'b', NULL))")

  private def allProducers: Seq[String] =
    arrayOfNull ++ mapWithNullValue ++ mapWithNullKey ++ arrayOfStructWithNull ++
      structWithNull ++ scalarNull ++ arrayOfStructWithNullField

  /**
   * Containers to wrap a producer in before putting it under a serializing operator. The nested
   * nullability mismatches only show where a container nests the value and an operator re-reads
   * the nesting: `collect_list(array(map(k, NULL)))` fails while both `array(map(k, NULL))` and
   * `collect_list(map(k, NULL))` pass.
   */
  private val nestingWrappers = Seq(
    "array(%s)",
    "array_repeat(%s, 2)",
    "element_at(%s, 1)",
    "slice(%s, 1, 1)",
    "map(id, %s)",
    "named_struct('s', %s)")

  private val excludedOptimizerRules = Seq(
    "ConstantFolding",
    "NullPropagation",
    "SimplifyBinaryComparison",
    "SimplifyConditionals").map("org.apache.spark.sql.catalyst.optimizer." + _).mkString(",")

  /**
   * How rows are cut into batches and which path an exchange takes. The expression sweeps vary
   * what is computed; these vary the machinery it runs through, because the defaults hide a whole
   * class of defects: 16 rows in one batch never reuse a builder, `REPARTITION(3)` never leaves
   * the JVM shuffle's bypass writer (one native call per batch), and a `spark.comet.batchSize` of
   * 8192 never puts a batch boundary inside a stateful expression.
   *
   * `rows` is the size of `t`; the shuffle profiles need enough rows per destination partition to
   * span several `spark.comet.shuffle.jvm.batchSize` batches inside one native call.
   */
  private case class Profile(name: String, rows: Int, confs: Seq[(String, String)])

  private val defaultProfile = Profile("default", 16, Seq.empty)

  /** Native batches of two rows, so every operator and stateful expression crosses a boundary. */
  private val smallBatchProfile = Profile(
    "small-batches",
    200,
    Seq(
      CometConf.COMET_BATCH_SIZE.key -> "2",
      CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE.key -> "2",
      CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true"))

  /**
   * Every registered array, map, struct and any-type aggregate serde opted into its native
   * kernel. An `Incompatible` serde routes through the JVM codegen dispatcher by default, so
   * without this profile its native kernel never runs in the sweep; `allowIncompatible` is what
   * lets a user reach it, and the kernel's NullType handling has to hold there too.
   */
  private lazy val allowIncompatibleProfile = Profile(
    "allow-incompatible",
    16,
    registeredSerdes.toSeq
      .map(cls => CometConf.getExprAllowIncompatConfigKey(cls) -> "true")
      .sortBy(_._1))

  /**
   * Profiles for the sweeps whose queries end in a projection: batching and the kernel choice are
   * what vary.
   */
  private lazy val kernelProfiles =
    Seq(defaultProfile, smallBatchProfile, allowIncompatibleProfile)

  /**
   * Profiles for the sweeps that put a value through an operator. Chosen so every pair of
   * settings below appears together at least once: JVM shuffle through the bypass writer
   * (partition count under `spark.shuffle.sort.bypassMergeThreshold`) and through the sort-based
   * writer (above it, one whole partition per native call), with and without forced spills,
   * native shuffle, AQE on and off, native columnar-to-row on and off, and batch sizes of two.
   */
  private val physicalProfiles = Seq(
    defaultProfile,
    Profile(
      "default-native-c2r",
      16,
      Seq(CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true")),
    smallBatchProfile.copy(confs = smallBatchProfile.confs ++ Seq(
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")),
    Profile(
      "jvm-sort-writer",
      900,
      Seq(
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.SHUFFLE_PARTITIONS.key -> "300",
        CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE.key -> "2",
        CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> "100000",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false")),
    Profile(
      "jvm-sort-writer-spills",
      900,
      Seq(
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.SHUFFLE_PARTITIONS.key -> "300",
        CometConf.COMET_BATCH_SIZE.key -> "2",
        CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE.key -> "2",
        CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> "10",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true")),
    Profile(
      "native-shuffle",
      900,
      Seq(
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        SQLConf.SHUFFLE_PARTITIONS.key -> "300",
        CometConf.COMET_BATCH_SIZE.key -> "2",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false")))

  private def rowsOf(
      query: String,
      cometEnabled: Boolean,
      ansi: Boolean,
      profile: Profile): (Seq[String], Boolean) = {
    val confs = Seq(
      CometConf.COMET_ENABLED.key -> cometEnabled.toString,
      CometConf.COMET_EXEC_ENABLED.key -> cometEnabled.toString,
      SQLConf.ANSI_ENABLED.key -> ansi.toString,
      // Keep the producers and their consumers out of the optimizer's hands so they are
      // evaluated per row by the engine under test rather than folded at plan time: the
      // producers are deterministic and non-nullable, so without this `p IS NULL` becomes
      // `false`, `p = p` and `p <=> p` become `true`, and `coalesce(p, p)` / `IF(c, p, p)`
      // collapse to `p`, leaving a projection of literals that proves nothing.
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedOptimizerRules) ++ profile.confs
    // withSQLConf's body is typed `=> Unit` on the older supported Spark versions and generic
    // only on the newer ones, so the result is captured out of band to stay portable.
    var result: (Seq[String], Boolean) = (Seq.empty, false)
    withSQLConf(confs: _*) {
      val df = spark.sql(query)
      val rows = df.collect().map(_.toString()).sorted.toSeq
      // The sweep's queries put the NullType expression in a projection, so a native
      // CometProjectExec is what distinguishes a case that actually exercised a native kernel
      // from one that merely fell back. Without this, a sweep where everything falls back would
      // still be green while proving nothing.
      val nativeProject =
        collectFirst(df.queryExecution.executedPlan) { case _: CometProjectExec =>
          ()
        }.isDefined
      result = (rows, nativeProject)
    }
    result
  }

  // ANSI is a dimension of the consumer sweeps because it changes which serdes wrap their child
  // in a null guard and which kernels raise on out-of-range access; the operators below carry
  // no ANSI semantics of their own.
  for (ansi <- Seq(false, true); profile <- kernelProfiles) {
    val tag = s"(ansi=$ansi ${profile.name})"
    test(s"NullType producers survive every consumer that Spark accepts $tag") {
      sweep(
        "consumer",
        cases().map { case (producer, expr) => (producer, s"SELECT $expr FROM t") },
        comparedFloor = 120,
        nativeFloor = 100,
        ansi = ansi,
        profile = profile)
    }

    // The plain producers are non-nullable, so this is the only sweep where a serde's null guard
    // runs natively over a NullType-bearing column that is NULL on some rows and takes its ELSE
    // branch; the non-deterministic sweep below makes the same serdes fall back instead.
    test(s"nullable deterministic NullType producers survive every consumer $tag") {
      sweep(
        "nullable",
        cases(nullableDeterministic).map { case (producer, expr) =>
          (producer, s"SELECT $expr FROM t")
        },
        comparedFloor = 140,
        nativeFloor = 110,
        ansi = ansi,
        profile = profile)
    }

    test(s"stateful NullType producers survive guards filtered by a sibling $tag") {
      sweep(
        "cross-input",
        crossInputCases.map { case (producer, expr) => (producer, s"SELECT $expr FROM t") },
        comparedFloor = 100,
        nativeFloor = 60,
        ansi = ansi,
        profile = profile)
    }

    test(s"nullable non-deterministic NullType producers survive every consumer $tag") {
      sweep(
        "non-deterministic",
        cases(nullableNondeterministic).map { case (producer, expr) =>
          (producer, s"SELECT $expr FROM t")
        },
        comparedFloor = 140,
        nativeFloor = 110,
        ansi = ansi,
        profile = profile)
    }
  }

  // Every physical profile, because the operator sweep is the one whose queries actually cut
  // rows into batches and push them through an exchange.
  for (profile <- physicalProfiles) {
    test(s"NullType producers survive every operator that Spark accepts (${profile.name})") {
      sweep(
        "operator",
        for (producer <- allProducers; (op, template) <- operatorTemplates)
          yield (op, template.replace("%s", producer)),
        comparedFloor = 160,
        nativeFloor = 100,
        profile = profile)
    }
  }

  /**
   * Registered array, map and struct serdes whose nested-typed argument no `NullType` producer
   * can occupy, so no consumer template can reach them: `Sequence` takes integral or temporal
   * bounds, and `StringToMap` and `JsonToStructs` take a string.
   */
  private lazy val serdesWithoutNestedInput: Set[Class[_ <: Expression]] =
    Set(classOf[Sequence], classOf[StringToMap], classOf[JsonToStructs])

  /** Aggregates whose serde accepts any input type, so a `NullType` shape can reach them. */
  /** The serdes the sweep must reach: those with a nested-typed argument plus the aggregates. */
  private lazy val registeredSerdes: Set[Class[_]] =
    (QueryPlanSerde.arrayExpressions.keySet ++
      QueryPlanSerde.mapExpressions.keySet ++
      QueryPlanSerde.structExpressions.keySet).toSet[Class[_]] --
      serdesWithoutNestedInput ++ anyTypeAggregates

  private lazy val anyTypeAggregates: Set[Class[_]] = Set(
    classOf[CollectList],
    classOf[CollectSet],
    classOf[Count],
    classOf[First],
    classOf[Last],
    classOf[Max],
    classOf[Min])

  /**
   * Every expression class in the analyzed and optimized plans of `query`, or none if Spark
   * rejects it. Both plans, because the optimizer is what inserts some expressions (`MapSort`
   * under a map grouping key) and what expands `RuntimeReplaceable` ones.
   */
  private def expressionClasses(query: String): Set[Class[_]] =
    Try {
      val qe = spark.sql(query).queryExecution
      Seq(qe.analyzed, qe.optimizedPlan)
    }.toOption.toSeq.flatten
      .toSet[LogicalPlan]
      .flatMap { plan =>
        plan.flatMap(_.expressions).flatMap { root =>
          root.collect { case e: Expression => e }.flatMap {
            case r: RuntimeReplaceable => r +: r.replacement.collect { case e: Expression => e }
            case e => Seq(e)
          }
        }
      }
      .map(_.getClass)

  // The consumer and operator lists are written by hand, so this is the check that a serde added
  // to the registry later, or one forgotten now, does not silently stay outside the sweep.
  test("the sweep reaches every registered array, map, struct and any-type aggregate serde") {
    withTempView("t") {
      spark.range(0, 8).createOrReplaceTempView("t")
      // `registeredSerdes` is derived from QueryPlanSerde's registries and feeds two protections:
      // this staleness check and `allowIncompatibleProfile`'s conf list. Were those registries
      // renamed or emptied, both would degrade silently in the same direction -- `missing` becomes
      // trivially empty and the profile collapses into a duplicate of `defaultProfile`. Pin the
      // size so that cannot pass unnoticed.
      assert(
        registeredSerdes.size >= 40,
        s"only ${registeredSerdes.size} serdes discovered; QueryPlanSerde's registries have moved " +
          "and both this check and the allow-incompatible profile are now vacuous")
      assert(
        allowIncompatibleProfile.confs.size == registeredSerdes.size,
        "the allow-incompatible profile must opt every serde the sweep reaches into its kernel")
      val reached =
        (cases().map { case (_, expr) =>
          s"SELECT $expr FROM t"
        } ++
          (for (producer <- allProducers; (_, template) <- operatorTemplates)
            yield template.replace("%s", producer)))
          .flatMap(expressionClasses)
          .toSet
      val missing = registeredSerdes -- reached
      assert(
        missing.isEmpty,
        s"registered serdes no sweep template reaches: ${missing
            .map(_.getSimpleName)
            .toSeq
            .sorted
            .mkString(", ")}")
    }
  }

  for (profile <- physicalProfiles) {
    test(s"nested NullType values survive the operators that serialize them (${profile.name})") {
      // Select producers by the type they actually have: `filter(array(CAST(NULL AS int)), ...)`
      // is `array<int>`, and nesting it only exercises a pre-existing nested-container limitation.
      val nullBearingProducers = allProducers.filter { p =>
        Try(spark.range(0, 8).selectExpr(s"$p AS c").schema.head.dataType).toOption
          .exists(SupportLevel.containsType(_, classOf[NullType]))
      }
      assert(
        nullBearingProducers.size >= 5,
        s"only ${nullBearingProducers.size} producers still carry a NullType; the producer list " +
          "has drifted away from what this sweep is meant to cover")
      val nested =
        for (producer <- nullBearingProducers; wrapper <- nestingWrappers)
          yield wrapper.replace("%s", producer)
      sweep(
        "nesting",
        for (value <- nested.distinct; (op, template) <- serializingOperators)
          yield (op, template.replace("%s", value)),
        comparedFloor = 200,
        nativeFloor = 100,
        profile = profile)
    }
  }

  /**
   * Runs every `(label, query)` twice, Comet off and on, and reports all divergences together. A
   * query Spark itself rejects is skipped; one whose Comet arm throws or disagrees is a failure.
   *
   * The floors are floors rather than equalities because which cases Spark accepts varies across
   * the supported Spark versions. `comparedFloor` fails a sweep whose templates have gone stale;
   * `nativeFloor` fails one where Comet fell back almost everywhere, since falling back is a pass
   * and such a sweep would prove nothing about the native kernels.
   */
  private def sweep(
      name: String,
      queries: Seq[(String, String)],
      comparedFloor: Int,
      nativeFloor: Int,
      ansi: Boolean = false,
      profile: Profile = defaultProfile): Unit = {
    var native = 0
    withTempPath { dir =>
      // One partition, so every batch holds several rows: a null guard over a non-deterministic
      // child only diverges where the CASE sees both matching and non-matching rows in one batch.
      spark.range(0, profile.rows, 1, 1).write.parquet(dir.getAbsolutePath)
      withTempView("t") {
        spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t")

        val failures = ArrayBuffer.empty[String]
        var compared = 0
        var skipped = 0

        for ((label, query) <- queries) {
          Try(rowsOf(query, cometEnabled = false, ansi, profile)).toOption match {
            case None =>
              skipped += 1
            case Some((sparkRows, _)) =>
              compared += 1
              Try(rowsOf(query, cometEnabled = true, ansi, profile)) match {
                case Failure(e) =>
                  failures += s"[threw:$label] $query\n              " +
                    s"${e.getClass.getSimpleName}: ${firstLine(causeText(e))}"
                case Success((cometRows, _)) if cometRows != sparkRows =>
                  failures += s"[mismatch:$label] $query\n" +
                    s"              spark: ${preview(sparkRows)}\n" +
                    s"              comet: ${preview(cometRows)}"
                case Success((_, nativeProject)) =>
                  if (nativeProject) native += 1
              }
          }
        }

        logWarning(
          s"NullType $name sweep (ansi=$ansi, profile=${profile.name}): " +
            s"compared=$compared native=$native skipped=$skipped total=${queries.length}")
        assert(
          compared >= comparedFloor,
          s"$name sweep only compared $compared of ${queries.length} cases; " +
            "its templates have gone stale")
        assert(
          native >= nativeFloor,
          s"$name sweep only executed $native cases natively; Comet is falling back almost " +
            "everywhere, so the comparison proves nothing about the native kernels")
        if (failures.nonEmpty) {
          // Name the profile: under `allow-incompatible` a mismatch can be a serde's *documented*
          // difference (array_intersect's element order, array_except's null handling) rather than
          // a NullType defect, and the two have to be told apart.
          fail(
            s"${failures.length} of $compared NullType $name cases diverge from Spark " +
              s"(ansi=$ansi, profile=${profile.name}, $skipped invalid in Spark):\n" +
              failures.mkString("\n"))
        }
      }
    }
  }

  private def causeText(e: Throwable): String = {
    val builder = new StringBuilder
    var current: Throwable = e
    var depth = 0
    while (current != null && depth < 10) {
      builder.append(Option(current.getMessage).getOrElse("")).append('\n')
      current = current.getCause
      depth += 1
    }
    builder.toString()
  }

  private def firstLine(message: String): String =
    Option(message).map(_.linesIterator.next()).getOrElse("<no message>")

  private def preview(rows: Seq[String]): String =
    rows.take(3).mkString(", ") + (if (rows.length > 3) s", ... (${rows.length} rows)" else "")
}
