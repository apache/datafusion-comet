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

import org.apache.logging.log4j.Level
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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.{CometConf, CometCoverageStats, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark42Plus}
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

  // Regression test for https://github.com/apache/datafusion-comet/issues/1389
  test("CometExecRule should not allow Comet partial and Spark final hash aggregate") {
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

        // COUNT is intentionally excluded from mixed execution (AQE / count-bug reasons), so if
        // the final aggregate cannot be converted to Comet, neither should the partial.
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
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

        // COUNT blocks mixed execution, so if the partial cannot be converted, neither should
        // the final.
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

  /**
   * Run `sql` with plan-only mode enabled and assert nothing was offloaded to native. `useV1`
   * toggles between `USE_V1_SOURCE_LIST=parquet` (V1 `CometScanExec` path) and
   * `USE_V1_SOURCE_LIST=""` (V2 `CometBatchScanExec` path).
   */
  private def runPlanOnlyAndAssertReverted(
      sql: String,
      useV1: Boolean = true,
      aqe: Boolean = true): Unit = {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) "parquet" else ""),
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
      val executed = spark.sql(sql).queryExecution.executedPlan
      val cometNodes = stripAQEPlan(executed).collect { case p: CometPlan => p }
      assert(
        cometNodes.isEmpty,
        s"plan-only mode must not offload; found Comet operators: $cometNodes")
    }
  }

  for {
    useV1 <- Seq(true, false)
    aqe <- Seq(true, false)
  } {
    val label = s"${if (useV1) "V1" else "V2"} scan, AQE=$aqe"
    test(s"plan-only mode: $label") {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        runPlanOnlyAndAssertReverted(
          "SELECT _2, count(*) FROM tbl GROUP BY _2",
          useV1 = useV1,
          aqe = aqe)
      }
    }
  }

  test("plan-only mode: scalar subquery is also reverted") {
    withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
      runPlanOnlyAndAssertReverted("SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)")
    }
  }

  test("plan-only mode: same query with the config off runs on Comet") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val plan =
          spark.sql("SELECT _2, count(*) FROM tbl GROUP BY _2").queryExecution.executedPlan
        val cometNodes = stripAQEPlan(plan).collect { case p: CometPlan => p }
        assert(cometNodes.nonEmpty, "expected Comet operators when plan-only mode is disabled")
      }
    }
  }

  private val PLAN_ONLY_PREFIX = "[Comet plan-only]"

  /** Runs `f` and returns the `[Comet plan-only]` reports that `CometExecRule` logged. */
  private def capturePlanOnlyReports(f: => Unit): Seq[String] = {
    val appender = new LogAppender("Comet plan-only reports")
    withLogAppender(
      appender,
      loggerNames = Seq(classOf[CometExecRule].getName),
      level = Some(Level.WARN)) {
      f
    }
    appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.startsWith(PLAN_ONLY_PREFIX))
      .toSeq
  }

  /** The `Comet accelerated N out of M eligible operators` counts in a plan-only report. */
  private def coverageOf(report: String): (Int, Int) = {
    val pattern = """Comet accelerated (\d+) out of (\d+) eligible operators""".r
    pattern
      .findFirstMatchIn(report)
      .map(m => (m.group(1).toInt, m.group(2).toInt))
      .getOrElse(fail(s"report has no coverage summary:\n$report"))
  }

  // The outer query is planned after any subquery it contains, so a report slot owned by the
  // first plan Spark prepares would describe the subquery and never the query being evaluated.
  for (aqe <- Seq(true, false)) {
    test(s"plan-only mode: report describes the outer query, not just a subquery (AQE=$aqe)") {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          val reports = capturePlanOnlyReports {
            spark.sql("SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)").collect()
          }
          assert(reports.nonEmpty, "expected a plan-only report")
          // The outer plan's Filter appears in no subquery plan, so its presence proves the
          // outer query was reported and not suppressed by the subquery's earlier planning.
          assert(
            reports.exists(_.contains("Filter")),
            s"no report describes the outer query:\n${reports.mkString("\n\n")}")
          assert(
            reports.distinct.size == reports.size,
            s"the same plan was reported more than once:\n${reports.mkString("\n\n")}")
          // Expected: one report for the subquery plan, one for the outer plan. AQE applies the
          // rule again per stage and per re-optimization; those must not add reports.
          assert(
            reports.size <= 4,
            s"expected a report per planned plan, got ${reports.size}:\n" +
              reports.mkString("\n\n"))
        }
      }
    }
  }

  test("plan-only mode: coverage accounts for post-columnar stage reversion") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      // AQE off so that Spark applies the post-columnar rules to the whole plan exactly once,
      // which is what the preview does, making the two directly comparable.
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val query = "SELECT _2, count(*), sum(_1) FROM tbl GROUP BY _2"

        // Comet accelerates part of this plan when the stage is left alone, so a preview that
        // stopped before the post-columnar rules would report a non-zero count below.
        withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
          val df = sql(query)
          df.collect()
          assert(CometCoverageStats.forPlan(df.queryExecution.executedPlan).cometOperators > 0)
        }

        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          // What Comet really executes with reversion enabled.
          val df = sql(query)
          df.collect()
          val executed = CometCoverageStats.forPlan(df.queryExecution.executedPlan)

          // The assertions stay inside the config block: on Spark 3.4 and 3.5 `withSQLConf` is
          // declared to return `Unit`, so a value cannot be carried out of one.
          withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
            val reports = capturePlanOnlyReports(sql(query).collect())
            assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
            assert(
              coverageOf(reports.head) ==
                (executed.cometOperators, executed.cometOperators + executed.sparkOperators),
              s"report disagrees with the executed plan ($executed):\n${reports.head}")
          }
        }
      }
    }
  }

  // `df.rdd.count()` and reading `executedPlan` without running an action can plan - and in the
  // first case execute AQE stages - without a SQL execution ID installed, so the reporting state
  // cannot be scoped to one. Each of these must still produce exactly one report per query, not
  // one per stage and re-optimization.
  for (aqe <- Seq(true, false)) {
    test(s"plan-only mode: one report per query without a SQL execution ID (AQE=$aqe)") {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          val query = "SELECT _2, count(*) FROM tbl GROUP BY _2"

          val viaRdd = capturePlanOnlyReports(spark.sql(query).rdd.count())
          assert(
            viaRdd.size == 1,
            s"expected one report for df.rdd.count(), got ${viaRdd.size}:\n" +
              viaRdd.mkString("\n\n"))

          val viaExecutedPlan =
            capturePlanOnlyReports(spark.sql(query).queryExecution.executedPlan)
          assert(
            viaExecutedPlan.size == 1,
            s"expected one report for executedPlan, got ${viaExecutedPlan.size}:\n" +
              viaExecutedPlan.mkString("\n\n"))
        }
      }
    }
  }

  // A scalar subquery is planned as a top-level plan of its own and substituted into the outer
  // plan, and extended explain counts the plans owned by a node's expressions. The outer preview
  // therefore has to preview its subqueries too, or it reports operators Comet does accelerate as
  // Spark. AQE is off here so the preview and the executed plan are the same single pass; see the
  // user guide for why the two can differ under AQE.
  test("plan-only mode: outer report coverage matches normal planning for a scalar subquery") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val query = "SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)"

        // What Comet really executes, subquery operators included.
        val df = sql(query)
        df.collect()
        val executed = CometCoverageStats.forPlan(df.queryExecution.executedPlan)
        assert(
          executed.cometOperators > 0,
          "test query must be partly accelerated for the comparison to mean anything")

        withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
          val reports = capturePlanOnlyReports(sql(query).collect())
          // One report for the separately planned subquery, one for the outer query. Only the
          // outer one describes the Filter.
          val outer = reports.filter(_.contains("Filter"))
          assert(
            outer.size == 1,
            s"expected exactly one report for the outer query, got ${outer.size}:\n" +
              reports.mkString("\n\n"))
          assert(
            coverageOf(outer.head) ==
              (executed.cometOperators, executed.cometOperators + executed.sparkOperators),
            s"outer report disagrees with the executed plan ($executed):\n${outer.head}")
        }
      }
    }
  }

}
