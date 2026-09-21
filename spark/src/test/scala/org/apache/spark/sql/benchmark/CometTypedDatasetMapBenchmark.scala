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

import java.nio.charset.StandardCharsets

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/** Top-level so `NewInstance` needs no outer pointer, which is the ordinary user shape. */
case class TypedMapRec(a: Long, b: String)

/**
 * Benchmark of `RewriteTypedDatasetMap`, which fuses the `SerializeFromObject` / `MapElements` /
 * `DeserializeToObject` sandwich a typed `Dataset.map` produces into a Comet projection routed
 * through the JVM codegen dispatcher. See https://github.com/apache/datafusion-comet/issues/5710.
 *
 * The question this exists to answer is where the rewrite pays for itself, because
 * `spark.comet.exec.typedDatasetMap.enabled` is off by default until it has an answer. Three
 * arms:
 *
 *   - `fuse off` -- today's default, where the sandwich is a Spark fallback island.
 *   - `fuse on` -- the rewrite. The user closure still runs on the JVM, once per row, but inside
 *     a Janino-compiled kernel reading and writing Arrow vectors, so the operators above the map
 *     stay native.
 *   - `Spark (Comet disabled)` -- a reference point, not the comparison of interest.
 *
 * '''Know what is actually at stake before reading a row.''' The premise behind the rewrite was
 * that the island's fallback cascades and costs every operator above it. On current main it
 * mostly does not: for `map -> group by` the fuse-off plan already keeps the exchange and the
 * final aggregate native, because Comet re-enters above the island via `CometColumnarShuffle`.
 * What fusing actually buys is the '''partial''' aggregate and an upgrade from
 * `CometColumnarShuffle` to `CometNativeShuffle`. So the cases are organised around how much that
 * partial aggregate is worth -- each grouped shape is run at both [[LoCardKeys]] and
 * [[HiCardKeys]] distinct keys -- and around what fusing has to pay for it: a struct vector built
 * and immediately unpacked, and every row materialised into Arrow even when a selective operator
 * above is about to discard it.
 *
 * Both Comet arms are consumed with `noop()`, which reads rows. That is not neutral between them
 * and is not meant to be: with the fuse off the plan is already row-based at the top, while with
 * it on the top is columnar and pays a columnar-to-row transition at the sink. That transition is
 * exactly the top-of-plan cost the feature flag exists to guard against, so charging the fused
 * arm for it is the honest measurement, not a confound.
 *
 * A `fuse off (repeat)` case repeats the baseline at the end of every table. It measures the same
 * work as the first row, so the spread between the two is this machine's noise floor for that
 * table; ignore any difference between the other rows that is smaller than that spread. This
 * matters more than usual here, because the arm that loses operators to Spark is the one most
 * sensitive to a busy machine.
 *
 * Before timing anything, every case is run through all three arms and the rows are compared, so
 * a timing cannot come from an arm that computed something else. Each case's plans are also
 * checked: a `fuse on` arm that is not fully native, or a `fuse off` arm that is, would not be
 * measuring what its name says.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTypedDatasetMapBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometTypedDatasetMapBenchmark-**results.txt".
 */
object CometTypedDatasetMapBenchmark extends CometBenchmarkBase {

  /**
   * ~512 batches at the default `spark.comet.batchSize`, so per-batch costs are amortised.
   *
   * Sized by the noise detector rather than by taste. At 128Ki rows an iteration of every case
   * here lands in the 15-25ms range on an M3 Max, and the `fuse off (repeat)` row came back up to
   * 27% away from the `fuse off` row it duplicates -- a spread wider than any difference between
   * the arms, which makes the whole table unreadable. Lower this on a smaller machine only if the
   * repeat row still agrees with the baseline afterwards.
   */
  private val Rows = 4 * 1024 * 1024

  // Declared rather than pulled in with `import spark.implicits._`, which would force the session
  // to start while this object is still initialising.
  private implicit val recEncoder: Encoder[TypedMapRec] = Encoders.product[TypedMapRec]
  private implicit val longEncoder: Encoder[Long] = Encoders.scalaLong

  /**
   * @param name
   *   Case name, as it appears in the results table.
   * @param build
   *   Builds the query from the typed Dataset. Kept as a function rather than SQL because there
   *   is no SQL spelling of a typed `map`.
   * @param hiCard
   *   Groups on a key with [[HiCardKeys]] distinct values instead of [[LoCardKeys]]. The partial
   *   aggregate is the one operator fusing actually rescues (see the class comment), so its cost
   *   is the variable that decides whether the rewrite can pay for itself at all.
   * @param fusesToNative
   *   False for a case the rewrite is expected to decline, where the point of the row is to show
   *   the decline costs nothing rather than to show a speedup.
   * @param extraConfigs
   *   Applied to all three arms, so they never account for a difference between them.
   */
  private case class MapCase(
      name: String,
      build: Dataset[TypedMapRec] => DataFrame,
      hiCard: Boolean = false,
      fusesToNative: Boolean = true,
      extraConfigs: Seq[(String, String)] = Nil)

  /**
   * Distinct grouping keys. At [[Rows]] rows the high-cardinality key averages 4 rows a group.
   */
  private val LoCardKeys = 100
  private val HiCardKeys = 1024 * 1024

  /**
   * AQE off and one shuffle partition for every case that shuffles, so both arms plan the same
   * shape on every iteration and the plan check is not looking at an `AQEShuffleRead`.
   */
  private val deterministicShuffle =
    Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", SQLConf.SHUFFLE_PARTITIONS.key -> "1")

  private def groupBySum(ds: Dataset[TypedMapRec]): DataFrame =
    ds.map(r => TypedMapRec(r.a + 1, r.b)).groupBy("b").agg(sum("a"))

  private def filterGroupBySum(ds: Dataset[TypedMapRec]): DataFrame =
    ds.map(r => TypedMapRec(r.a * 2, r.b)).filter(col("a") % 3 === 0).groupBy("b").agg(sum("a"))

  // Ordered so each low-cardinality case sits next to its high-cardinality twin and the pair is
  // measured under similar machine conditions, and so the decisive pairs run before the tables
  // that contention is most likely to spoil. Spark's `Benchmark` warms every case separately, so
  // ordering does not otherwise affect a reading.
  private def cases: Seq[MapCase] = Seq(
    // The case the rewrite exists for. On current main the fuse-off plan already keeps the
    // exchange and the *final* aggregate native -- Comet re-enters above the island -- so the only
    // operator fusing rescues here is the partial aggregate, plus an upgrade from
    // `CometColumnarShuffle` to `CometNativeShuffle`. With 100 keys that partial aggregate is
    // nearly free, which is why this row says so little. Both output columns are consumed so
    // column pruning cannot give the two arms different work to do.
    MapCase(
      s"map -> group by, $LoCardKeys keys",
      groupBySum,
      extraConfigs = deterministicShuffle),
    // The same shape with a partial aggregate that actually costs something: a million-entry hash
    // table instead of a hundred. This is the fair test of the PR's premise. If fusing cannot win
    // here it cannot win on aggregate rescue at all, because this is the most the rescued operator
    // can be worth.
    MapCase(
      s"map -> group by, $HiCardKeys keys",
      groupBySum,
      hiCard = true,
      extraConfigs = deterministicShuffle),
    // A filter between the map and the aggregate. The fused kernel must materialise every row into
    // Arrow -- including the string column -- before `CometFilter` discards two thirds of them,
    // where whole-stage codegen drops them inside the same row loop.
    MapCase(
      s"map -> filter -> group by, $LoCardKeys keys",
      filterGroupBySum,
      extraConfigs = deterministicShuffle),
    // ...and with an expensive partial aggregate, to see whether rescuing it outweighs paying for
    // the rows the filter is about to throw away.
    MapCase(
      s"map -> filter -> group by, $HiCardKeys keys",
      filterGroupBySum,
      hiCard = true,
      extraConfigs = deterministicShuffle),
    // Nothing above the map. The rewrite has no operator to rescue and pays a columnar-to-row
    // transition at the sink that the unfused plan does not, so this is the case the default-off
    // decision rests on. One output column takes the direct path: a single fused expression, no
    // struct wrapper.
    MapCase("map -> sink, 1 col", _.map(_.a + 1).toDF()),
    // Two output columns take the `CreateNamedStruct` + `GetStructField` path, which builds a
    // struct vector and immediately reads the fields back out. Worth separating from the row
    // above: if the struct path is much worse at the top of the plan, that is a cost of the
    // multi-column encoding rather than of fusing as such.
    MapCase("map -> sink, 2 cols", _.map(r => TypedMapRec(r.a + 1, r.b)).toDF()),
    // `ds.map(f).map(g)` leaves two adjacent `MapElements` under one Serialize/Deserialize pair,
    // which the rule fuses as a whole. Two closure calls per row against one bridge crossing, so
    // the bridge is amortised further here than anywhere else in the table.
    MapCase(
      "map -> map -> group by",
      _.map(r => TypedMapRec(r.a + 1, r.b))
        .map(r => TypedMapRec(r.a * 2, r.b))
        .groupBy("b")
        .agg(sum("a")),
      extraConfigs = deterministicShuffle),
    // `mapPartitions` is iterator-shaped, so there is no per-row expression and the rule declines.
    // Included as a control: both Comet arms should land on the same plan and the same time, and a
    // difference between them would mean the rewrite is reaching something it should not.
    MapCase(
      "mapPartitions (declined)",
      _.mapPartitions(it => it.map(r => TypedMapRec(r.a + 1, r.b))).groupBy("b").agg(sum("a")),
      fusesToNative = false,
      extraConfigs = deterministicShuffle))

  private def sparkConfigs(c: MapCase): Seq[(String, String)] =
    Seq(CometConf.COMET_ENABLED.key -> "false") ++ c.extraConfigs

  /** Comet on, rewrite on: the behaviour this benchmark is validating. */
  private def fusedConfigs(c: MapCase): Seq[(String, String)] = cometConfigs(c, fuse = true)

  /** Comet on, rewrite off: today's default, where the typed map is a Spark fallback island. */
  private def unfusedConfigs(c: MapCase): Seq[(String, String)] = cometConfigs(c, fuse = false)

  private def cometConfigs(c: MapCase, fuse: Boolean): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    // The rewrite has no dispatcher to fuse into without this, and it is on by default; set it
    // explicitly in both Comet arms so the table does not depend on that default.
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true",
    CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> fuse.toString) ++ c.extraConfigs

  private val FusedCaseName = "Comet, typed map fused"
  private val UnfusedCaseName = "Comet, fuse off (Spark fallback island)"
  private val SparkCaseName = "Spark (Comet disabled)"

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val selected = cases
    runBenchmark("Typed Dataset map fusion: environment") {
      emitEnvironment(selected)
    }
    withCorpus(Rows) {
      selected.foreach(verifyArmsAgree)
      selected.foreach(runSteadyState)
    }
  }

  /**
   * The reader of a results file cannot see the confs the numbers were produced under, and for
   * this benchmark the feature flag and the batch size are the whole point.
   */
  private def emitEnvironment(selected: Seq[MapCase]): Unit = {
    emit(s"Spark version: ${spark.version}")
    emit(
      s"Java version: ${System.getProperty("java.version")} " +
        s"(${System.getProperty("java.vm.name")})")
    emit(s"Scala version: ${scala.util.Properties.versionNumberString}")
    emit(s"spark.master: ${spark.conf.get("spark.master", "<unset>")}")
    emit(
      s"${CometConf.COMET_BATCH_SIZE.key}: " +
        CometConf.COMET_BATCH_SIZE.get(spark.sessionState.conf))
    emit(
      s"${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}: " +
        spark.conf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key))
    emit(s"Feature flag: ${CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key}")
    emit(s"Dispatcher conf: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key} (true in both arms)")
    emit("Steady-state tables: Spark's Benchmark defaults -- 2s of untimed warmup per case, then")
    emit(
      "  at least 2 iterations and at least 2s of timed iterations; the table reports best and")
    emit("  average of the timed iterations.")
    emit(
      s"Rows: $Rows. Grouping key has $LoCardKeys distinct values, or $HiCardKeys in the " +
        "high-cardinality cases.")
    emit(s"Cases: ${selected.map(_.name).mkString(", ")}")
  }

  /**
   * Fails if the three arms disagree on a case. Rows are compared as a sorted multiset rather
   * than positionally, because the grouped cases shuffle and their output order is a property of
   * the plan, which is the one thing that differs between the arms.
   */
  private def verifyArmsAgree(c: MapCase): Unit = {
    def collect(configs: Seq[(String, String)]): Array[String] = {
      // Assigned to a local rather than returned from the block: Spark 3.4 and 3.5 declare
      // `SQLHelper.withSQLConf` as returning `Unit`; only Spark 4 has the result-returning form.
      var collected: Array[Row] = Array.empty
      withSQLConf(configs: _*) {
        collected = c.build(typedDataset(c.hiCard)).collect()
      }
      collected.map(_.toSeq.map(String.valueOf).mkString("|")).sorted
    }

    val expected = collect(sparkConfigs(c))
    Seq(FusedCaseName -> fusedConfigs(c), UnfusedCaseName -> unfusedConfigs(c)).foreach {
      case (armName, configs) =>
        val actual = collect(configs)
        assert(
          expected.length == actual.length,
          s"${c.name}: Spark produced ${expected.length} rows, $armName ${actual.length}")
        expected.indices.find(i => expected(i) != actual(i)).foreach { i =>
          throw new AssertionError(
            s"${c.name}: row $i differs -- Spark ${expected(i)}, $armName ${actual(i)}")
        }
    }
  }

  private def runSteadyState(c: MapCase): Unit = {
    runBenchmark(s"${c.name} -- $Rows rows") {
      val benchmark = new Benchmark(s"${c.name} -- $Rows rows", Rows, output = output)
      checkPlans(benchmark, c)
      // The unfused arm goes first so the `Relative` column reads as the speedup this change buys
      // over the behaviour that ships today.
      benchmark.addCase(UnfusedCaseName)(_ => runCase(c, unfusedConfigs(c)))
      benchmark.addCase(FusedCaseName)(_ => runCase(c, fusedConfigs(c)))
      benchmark.addCase(SparkCaseName)(_ => runCase(c, sparkConfigs(c)))
      benchmark.addCase(s"$UnfusedCaseName (repeat)")(_ => runCase(c, unfusedConfigs(c)))
      benchmark.run()
    }
  }

  /**
   * Warns rather than fails, so one Spark version planning a case differently degrades the table
   * to a note instead of aborting the run.
   */
  private def checkPlans(benchmark: Benchmark, c: MapCase): Unit = {
    var fusedNonComet: Option[String] = None
    var unfusedIsFullyComet = false
    withSQLConf(fusedConfigs(c): _*) {
      val df = c.build(typedDataset(c.hiCard))
      df.noop()
      fusedNonComet =
        findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan)).map(_.nodeName)
    }
    withSQLConf(unfusedConfigs(c): _*) {
      val df = c.build(typedDataset(c.hiCard))
      df.noop()
      unfusedIsFullyComet =
        findFirstNonCometOperator(stripAQEPlan(df.queryExecution.executedPlan)).isEmpty
    }
    if (c.fusesToNative) {
      fusedNonComet.foreach(op =>
        warn(
          benchmark,
          "WARNING: the fused plan is not fully Comet native (first non-Comet operator: " +
            s"$op), so that case is partly measuring Spark."))
      if (unfusedIsFullyComet) {
        warn(
          benchmark,
          "WARNING: the fuse-off plan is fully Comet native, so this case is not exercising " +
            "the fallback island it is meant to be compared against.")
      }
    } else if (fusedNonComet.isEmpty) {
      warn(
        benchmark,
        "WARNING: this case is supposed to be declined by the rewrite, but its fused plan is " +
          "fully Comet native, so the two Comet arms are not the control they claim to be.")
    }
  }

  private def runCase(c: MapCase, configs: Seq[(String, String)]): Unit =
    withSQLConf(configs: _*) {
      c.build(typedDataset(c.hiCard)).noop()
    }

  /**
   * The corpus read back as a typed `Dataset`. Rebuilt per call rather than cached, because the
   * plan it produces has to be built under the arm's own confs.
   */
  private def typedDataset(hiCard: Boolean): Dataset[TypedMapRec] = {
    val key = if (hiCard) "c_hi" else "c_lo"
    spark.sql(s"select c_a as a, $key as b from parquetV1Table").as[TypedMapRec]
  }

  /** Builds `parquetV1Table` with `rows` rows of the corpus and drops it afterwards. */
  private def withCorpus(rows: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(rows).createOrReplaceTempView(tbl)
        // `c_a` varies per row so the closure's arithmetic is not loop-invariant. `c_lo` makes the
        // partial aggregate nearly free and `c_hi` makes it expensive, which is the variable the
        // high-cardinality cases exist to move.
        prepareTable(
          dir,
          spark.sql(
            s"SELECT id AS c_a, CAST(PMOD(id, $LoCardKeys) AS STRING) AS c_lo, " +
              s"CAST(PMOD(id, $HiCardKeys) AS STRING) AS c_hi FROM $tbl"))
        f
      }
    }
  }

  /** Writes a warning to the results file as well as the console, ordered against the table. */
  private def warn(benchmark: Benchmark, message: String): Unit = {
    val border = "=" * 80
    benchmark.out.println(s"\n$border\n$message\n$border")
  }

  /** [[Benchmark]] tees console and results file; this benchmark's own tables need the same. */
  private def emit(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes(StandardCharsets.UTF_8)))
  }
}
