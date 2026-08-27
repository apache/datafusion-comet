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

package org.apache.comet.rules

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark42Plus}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

/**
 * Test suite specifically for CometExecRule transformation logic. Tests the rule's ability to
 * transform Spark operators to Comet operators, fallback mechanisms, configuration handling, and
 * edge cases.
 */
class CometExecRuleSuite extends CometTestBase {

  /** Helper method to apply CometExecRule and return the transformed plan */
  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    CometExecRule(spark).apply(stripAQEPlan(plan))
  }

  /** Create a test data frame that is used in all tests */
  private def createTestDataFrame = {
    val testSchema = new StructType(
      Array(
        StructField("id", DataTypes.IntegerType, nullable = true),
        StructField("name", DataTypes.StringType, nullable = true)))
    FuzzDataGenerator.generateDataFrame(new Random(42), spark, testSchema, 100, DataGenOptions())
  }

  /** Create a SparkPlan from the specified SQL with Comet disabled */
  private def createSparkPlan(spark: SparkSession, sql: String): SparkPlan = {
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.sql(sql)
      sparkPlan = df.queryExecution.executedPlan
    }
    sparkPlan
  }

  /** Count the number of the specified operator in the plan */
  private def countOperators(plan: SparkPlan, opClass: Class[_]): Int = {
    stripAQEPlan(plan).collect {
      case stage: QueryStageExec =>
        countOperators(stage.plan, opClass)
      case op if op.getClass.isAssignableFrom(opClass) => 1
    }.sum
  }

  test("expression-level fallback reasons are rolled up onto the operator that falls back") {
    // Extended explain only walks plan nodes, so a reason recorded on a sub-expression is
    // invisible unless CometExecRule lifts it onto the enclosing operator. Disabling a single
    // expression makes the Project fall back with the reason living on the Multiply node.
    // See https://github.com/apache/datafusion-comet/issues/5230.
    // This also pins the ordering inside `convertToComet`: strict mode is on in CometTestBase, so
    // if the roll-up stopped running before the strict check, planning here would throw.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id * 2 as doubled FROM test_data")
      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)

      withSQLConf(
        CometConf.getExprEnabledConfigKey("Multiply") -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        val project = stripAQEPlan(transformedPlan).collectFirst { case p: ProjectExec => p }.get

        val reasons = project
          .getTagValue(CometExplainInfo.FALLBACK_REASONS)
          .getOrElse(Set.empty[String])
        assert(
          reasons.exists(_.contains("Multiply")),
          s"expected the Multiply reason on the ProjectExec, got: $reasons")
        // The generic catch-all message must not appear: a real reason was available.
        assert(
          !reasons.exists(_.contains("is not supported")),
          s"a real reason was available but the generic message was used too: $reasons")
      }
    }
  }

  test("strict mode fails an operator that Comet declined without recording a reason") {
    // The bug this guards against is a serde returning None and forgetting to say why, which the
    // generic "<operator> is not supported" message used to hide. No serde in the tree is in that
    // state (the whole test corpus runs with strict mode on, which is what enforces it), so drive
    // the check directly with the shape such a serde produces: a handled operator whose children
    // are all native and which carries no reason on itself or its expressions.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id * 2 as doubled FROM test_data")
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val nativeChild = stripAQEPlan(applyCometExecRule(sparkPlan)).collectFirst {
          case op: CometNativeExec => op
        }.get
        val rule = CometExecRule(spark)

        // ProjectExec has a registered serde, so Comet did attempt this operator.
        val strictOp = ProjectExec(nativeChild.output, nativeChild)
        assert(CometExecRule.allExecs.contains(strictOp.getClass))
        val e = intercept[IllegalStateException] {
          rule.reportUnexplainedFallback(strictOp)
        }
        assert(e.getMessage.contains("recorded no fallback reason"))
        assert(e.getMessage.contains(strictOp.nodeName))

        // Production default: no throw, and the generic message so users still see something.
        withSQLConf(CometConf.COMET_STRICT_FALLBACK_REASONS.key -> "false") {
          val lenientOp = ProjectExec(nativeChild.output, nativeChild)
          rule.reportUnexplainedFallback(lenientOp)
          val reasons = lenientOp
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty[String])
          assert(reasons == Set(s"${lenientOp.nodeName} is not supported"))
        }
      }
    }
  }

  test("strict fallback reason checking is off by default and on for Comet's own suites") {
    // The strict check turns "a serde returned None without saying why" into a hard failure. It
    // must stay off in production, where the generic "<operator> is not supported" message is the
    // right user-facing behaviour, and on for every Comet suite so the bug class cannot ship
    // again. Enabling it in CometTestBase is what actually exercises it: the whole test corpus
    // runs with it on. See https://github.com/apache/datafusion-comet/issues/5230.
    assert(!CometConf.COMET_STRICT_FALLBACK_REASONS.defaultValue.get)
    assert(CometConf.COMET_STRICT_FALLBACK_REASONS.get(spark.sessionState.conf))
  }

  test("strict mode does not fire for operators Comet never attempted to convert") {
    // Strict mode must only fire when a serde actually attempted the operator and declined. An
    // operator Comet has no handler for was never attempted, so demanding a specific reason
    // would be wrong - it keeps the generic message.
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(spark, "SELECT id FROM test_data")
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "false") {
        // With local table scan disabled the leaf has no Comet handler applied, and planning
        // must complete rather than throw.
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(transformedPlan != null)
      }
    }
  }

  test(
    "CometExecRule should apply basic operator transformations, but only when Comet is enabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

      // Count original Spark operators
      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)
      assert(countOperators(sparkPlan, classOf[FilterExec]) == 1)

      for (cometEnabled <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

          val transformedPlan = applyCometExecRule(sparkPlan)

          if (cometEnabled) {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 1)
          } else {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 0)
          }
        }
      }
    }
  }

  test("CometExecRule should apply hash aggregate transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometHashAggregateExec]) == originalHashAggCount)
      }
    }
  }

  test("CometExecRule should allow COUNT Comet partial and Spark final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // COUNT's buffer is compatible in this direction. Keeping the Final in Spark also keeps
        // the AQE/count-bug rewrites that prevent the reverse direction from being admitted.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
      }
    }
  }

  test("CometExecRule should not allow Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // COUNT still blocks Spark Partial to Comet Final, independently of the safe reverse
        // direction, so if the partial cannot be converted, neither should the final.
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow safe Comet partial and Spark final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // Query uses only safe aggregates (MIN, MAX) with compatible intermediate buffers
      val sparkPlan =
        createSparkPlan(spark, "SELECT MIN(id), MAX(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Safe aggregates allow mixed execution: partial can be Comet, final stays Spark
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow safe Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // Query uses only safe aggregates (MIN, MAX) with compatible intermediate buffers
      val sparkPlan =
        createSparkPlan(spark, "SELECT MIN(id), MAX(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Safe aggregates allow mixed execution: partial stays Spark, final can be Comet
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should allow SUM mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT SUM(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // SUM buffer matches Spark: partial converts to Comet, final stays Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow SUM mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT SUM(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should allow AVG mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should not allow try_sum mixed execution") {
    assume(isSpark35Plus, "try_sum was added in Spark 3.5")
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT try_sum(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // TRY-mode SUM uses a Comet-internal buffer column, so mixing is unsafe:
        // the partial must also fall back to Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should not allow decimal AVG mixed execution") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      // Precision must be large enough (prec + 4 > 15) that Spark's own DecimalAggregates
      // optimizer rule does not rewrite AVG to operate on the unscaled Long value, which would
      // sidestep the decimal buffer path this test is meant to exercise.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT AVG(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // Decimal AVG is deferred (its overflow path nulls count differently from Spark), so
        // mixed execution is unsafe and the partial must also fall back to Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  for (distinct <- Seq(false, true)) {
    test(
      s"unsafe aggregate buffers fall back when native shuffle is ineligible (distinct=$distinct)") {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")
        val aggregates = "AVG(CAST(id AS DECIMAL(20, 2)))" +
          (if (distinct) ", COUNT(DISTINCT name)" else "")

        for (fallback <- Seq("disabled hash partitioning", "prior shuffle fallback", "none")) {
          withSQLConf(
            CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native",
            CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key ->
              (fallback != "disabled hash partitioning").toString) {
            val sparkPlan =
              createSparkPlan(spark, s"SELECT $aggregates FROM test_data GROUP BY (id % 3)")
            val aggregateCount = countOperators(sparkPlan, classOf[HashAggregateExec])
            assert(aggregateCount == (if (distinct) 4 else 2))
            if (fallback == "prior shuffle fallback") {
              foreach(sparkPlan) {
                case shuffle: ShuffleExchangeExec =>
                  withFallbackReason(shuffle, "prior shuffle fallback")
                case _ =>
              }
            }
            val transformed = applyCometExecRule(sparkPlan)

            // Shuffle is enabled, but a native-only shuffle can still fall back. The distinct
            // rewrite also has intermediate PartialMerge and mixed Partial/PartialMerge stages.
            val nativeExpected = fallback == "none"
            for (plan <- Seq(transformed, applyCometExecRule(transformed))) {
              assert(
                countOperators(plan, classOf[CometHashAggregateExec]) ==
                  (if (nativeExpected) aggregateCount else 0))
              assert(
                countOperators(plan, classOf[HashAggregateExec]) ==
                  (if (nativeExpected) 0 else aggregateCount))
            }
            // AQE reapplies the rule to an exchange without its Final aggregate. The tagged
            // Partial must remain in Spark in that stage-only pass too.
            transformed.collect { case shuffle: ShuffleExchangeExec => shuffle }.foreach {
              shuffle =>
                val stage = applyCometExecRule(shuffle)
                assert(countOperators(stage, classOf[CometHashAggregateExec]) == 0)
            }
          }
        }
      }
    }
  }

  test("CometExecRule should not allow decimal SUM mixed execution") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      // Precision must be large enough (prec + 4 > 15) that Spark's own DecimalAggregates
      // optimizer rule does not rewrite SUM to operate on the unscaled Long value, which would
      // sidestep the decimal buffer path this test is meant to exercise.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT SUM(CAST(id AS DECIMAL(20, 2))) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        // Decimal SUM overflow detection (ANSI throw / Legacy null) does not survive a
        // Spark-partial / Comet-final split, so mixed execution is unsafe and the partial
        // must also fall back to Spark.
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow AVG mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")
      val sparkPlan =
        createSparkPlan(spark, "SELECT AVG(id) FROM test_data GROUP BY (id % 3)")
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should allow BloomFilter mixed Comet partial and Spark final") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
    val funcId = new FunctionIdentifier("bloom_filter_agg")
    spark.sessionState.functionRegistry.registerFunction(
      funcId,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        })
    try {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")

        // Cast to bigint: Spark 3.4's bloom_filter_agg only accepts a long-typed first
        // argument; later versions widened it to any integral type.
        val sparkPlan =
          createSparkPlan(spark, "SELECT bloom_filter_agg(CAST(id AS BIGINT)) FROM test_data")

        val originalObjectAggCount = countOperators(sparkPlan, classOf[ObjectHashAggregateExec])
        assert(originalObjectAggCount == 2)

        withSQLConf(
          CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val transformedPlan = applyCometExecRule(sparkPlan)

          // BloomFilter is mixed-safe: partial converts to Comet, final stays Spark.
          assert(countOperators(transformedPlan, classOf[ObjectHashAggregateExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
        }
      }
    } finally {
      spark.sessionState.functionRegistry.dropFunction(funcId)
    }
  }

  test("CometExecRule should allow BloomFilter mixed Spark partial and Comet final") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
    val funcId = new FunctionIdentifier("bloom_filter_agg")
    spark.sessionState.functionRegistry.registerFunction(
      funcId,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        })
    try {
      withTempView("test_data") {
        createTestDataFrame.createOrReplaceTempView("test_data")

        // Cast to bigint: Spark 3.4's bloom_filter_agg only accepts a long-typed first
        // argument; later versions widened it to any integral type.
        val sparkPlan =
          createSparkPlan(spark, "SELECT bloom_filter_agg(CAST(id AS BIGINT)) FROM test_data")

        val originalObjectAggCount = countOperators(sparkPlan, classOf[ObjectHashAggregateExec])
        assert(originalObjectAggCount == 2)

        withSQLConf(
          CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
          val transformedPlan = applyCometExecRule(sparkPlan)

          assert(countOperators(transformedPlan, classOf[ObjectHashAggregateExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1)
        }
      }
    } finally {
      spark.sessionState.functionRegistry.dropFunction(funcId)
    }
  }

  // Regression tests for https://github.com/apache/datafusion-comet/issues/4813. An aggregate with
  // an incompatible intermediate buffer (percentile_approx) combined with a distinct aggregate is
  // rewritten by Spark into a multi-stage plan whose partial is separated from the final by
  // intermediate PartialMerge stages. If part of that chain runs in Comet and part in Spark the
  // incompatible buffer crosses the boundary and crashes, so the whole chain must fall back.
  test(
    "CometExecRule should not split distinct aggregate with incompatible buffer (Spark final)") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT percentile_approx(id, 0.5), COUNT(DISTINCT name) FROM test_data")

      // The distinct rewrite produces a multi-stage ObjectHashAggregate chain.
      assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 1)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // percentile_approx has an incompatible buffer, so with the final forced to Spark the
        // entire partial/merge chain must also stay in Spark.
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test(
    "CometExecRule should not split distinct aggregate with incompatible buffer (Spark part)") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT percentile_approx(id, 0.5), COUNT(DISTINCT name) FROM test_data")

      assert(countOperators(sparkPlan, classOf[ObjectHashAggregateExec]) > 1)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // With the partial/merge stages forced to Spark, no Comet aggregate may consume their
        // incompatible buffers either.
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should allow approx_count_distinct mixed Comet partial and Spark final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      // approx_count_distinct stores its registers in Spark's identical packed-Long buffer, so
      // it is mixed-safe: the Comet partial can feed a Spark final.
      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT approx_count_distinct(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // final only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // partial
      }
    }
  }

  test("CometExecRule should allow approx_count_distinct mixed Spark partial and Comet final") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(
          spark,
          "SELECT approx_count_distinct(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 1) // partial only
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 1) // final
      }
    }
  }

  test("CometExecRule should not convert hash aggregate when grouping key contains map type") {
    // Spark 3.4/3.5 reject `array<map<...>>` as a grouping key in the analyzer (not orderable),
    // so the plan never reaches CometExecRule on those versions. The guard we're exercising
    // (containsMapType) only matters on Spark 4.0+, which permits the GROUP BY to be analyzed.
    assume(isSpark40Plus)
    // Arrow's row format, used by DataFusion's grouped hash aggregate for composite keys, does
    // not support Map at any nesting level. Grouping by a type that transitively contains a map
    // (e.g. array<map<int,int>>) must stay on Spark to avoid a native row-encoding crash.
    val sparkPlan = createSparkPlan(
      spark,
      """SELECT count(*)
        |FROM VALUES (ARRAY(MAP(1, 2), MAP(1, 3))),
        |            (ARRAY(MAP(2, 3), MAP(1, 3))) AS t(a)
        |GROUP BY a""".stripMargin)

    val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
    assert(originalHashAggCount == 2)

    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      val transformedPlan = applyCometExecRule(sparkPlan)

      assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
      assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
    }
  }

  test("CometExecRule should apply broadcast exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT /*+ BROADCAST(b) */ a.id, b.name FROM test_data a JOIN test_data b ON a.id = b.id")

      // Count original Spark operators
      val originalBroadcastExchangeCount =
        countOperators(sparkPlan, classOf[BroadcastExchangeExec])
      assert(originalBroadcastExchangeCount == 1)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[BroadcastExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometBroadcastExchangeExec]) == originalBroadcastExchangeCount)
      }
    }
  }

  test("CometExecRule should apply shuffle exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, COUNT(*) FROM test_data GROUP BY id ORDER BY id")

      // Count original Spark operators
      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometShuffleExchangeExec]) == originalShuffleExchangeCount)
      }
    }
  }

  test("CometExecRule should not wrap shuffle in CometColumnarShuffle when both sides are JVM") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Disable partial aggregate so both aggregates fall back to Spark JVM. The shuffle between
      // them would otherwise be wrapped with CometColumnarShuffle, which adds unnecessary
      // row<->arrow conversion overhead when neither side can consume columnar output.
      // See https://github.com/apache/datafusion-comet/issues/4004.
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Both aggregates should remain JVM
        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)

        // The shuffle should remain a Spark ShuffleExchangeExec (not wrapped in Comet)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 0)
        assert(
          countOperators(transformedPlan, classOf[ShuffleExchangeExec]) ==
            originalShuffleExchangeCount)
      }
    }
  }

  test("CometExecRule should not revert columnar shuffle when the revert config is disabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Both aggregates fall back to JVM as in the prior test, but the revert optimization is
      // disabled, so the shuffle should still be wrapped in CometColumnarShuffle.
      withSQLConf(
        CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false",
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 1)
      }
    }
  }

  test("CometExecRule should not revert columnar shuffle when both aggregates go native") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // With default settings both aggregates convert to Comet native, so the shuffle between
      // them has a Comet consumer on both sides and must remain columnar - the revert must not
      // fire here.
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 2)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 1)
      }
    }
  }

}
