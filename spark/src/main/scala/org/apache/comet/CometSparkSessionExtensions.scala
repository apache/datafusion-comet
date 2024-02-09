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

import java.nio.ByteOrder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{isANSIEnabled, isCometEnabled, isCometExecEnabled, isCometOperatorEnabled, isCometScan, isCometScanEnabled, isSchemaSupported}
import org.apache.comet.parquet.{CometParquetScan, SupportsComet}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.shims.ShimCometSparkSessionExtensions

class CometSparkSessionExtensions
    extends (SparkSessionExtensions => Unit)
    with Logging
    with ShimCometSparkSessionExtensions {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar { session => CometScanColumnar(session) }
    extensions.injectColumnar { session => CometExecColumnar(session) }
    extensions.injectQueryStagePrepRule { session => CometScanRule(session) }
    extensions.injectQueryStagePrepRule { session => CometExecRule(session) }
  }

  case class CometScanColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometScanRule(session)
  }

  case class CometExecColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometExecRule(session)
  }

  case class CometScanRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!isCometEnabled(conf) || !isCometScanEnabled(conf)) plan
      else {
        plan.transform {
          // data source V2
          case scanExec: BatchScanExec
              if scanExec.scan.isInstanceOf[ParquetScan] &&
                isSchemaSupported(scanExec.scan.asInstanceOf[ParquetScan].readDataSchema) &&
                isSchemaSupported(scanExec.scan.asInstanceOf[ParquetScan].readPartitionSchema) &&
                // Comet does not support pushedAggregate
                getPushedAggregate(scanExec.scan.asInstanceOf[ParquetScan]).isEmpty =>
            val cometScan = CometParquetScan(scanExec.scan.asInstanceOf[ParquetScan])
            logInfo("Comet extension enabled for Scan")
            CometBatchScanExec(
              scanExec.copy(scan = cometScan),
              runtimeFilters = scanExec.runtimeFilters)

          // iceberg scan
          case scanExec: BatchScanExec =>
            if (isSchemaSupported(scanExec.scan.readSchema())) {
              scanExec.scan match {
                case s: SupportsComet if s.isCometEnabled =>
                  logInfo(s"Comet extension enabled for ${scanExec.scan.getClass.getSimpleName}")
                  // When reading from Iceberg, we automatically enable type promotion
                  SQLConf.get.setConfString(COMET_SCHEMA_EVOLUTION_ENABLED.key, "true")
                  CometBatchScanExec(
                    scanExec.clone().asInstanceOf[BatchScanExec],
                    runtimeFilters = scanExec.runtimeFilters)
                case _ =>
                  logInfo(
                    "Comet extension is not enabled for " +
                      s"${scanExec.scan.getClass.getSimpleName}: not enabled on data source side")
                  scanExec
              }
            } else {
              logInfo(
                "Comet extension is not enabled for " +
                  s"${scanExec.scan.getClass.getSimpleName}: Schema not supported")
              scanExec
            }

          // data source V1
          case scanExec @ FileSourceScanExec(
                HadoopFsRelation(_, partitionSchema, _, _, _: ParquetFileFormat, _),
                _: Seq[AttributeReference],
                requiredSchema,
                _,
                _,
                _,
                _,
                _,
                _) if isSchemaSupported(requiredSchema) && isSchemaSupported(partitionSchema) =>
            logInfo("Comet extension enabled for v1 Scan")
            CometScanExec(scanExec, session)
        }
      }
    }
  }

  case class CometExecRule(session: SparkSession) extends Rule[SparkPlan] {
    private def isCometNative(op: SparkPlan): Boolean = op.isInstanceOf[CometNativeExec]

    // spotless:off
    /**
     * Tries to transform a Spark physical plan into a Comet plan.
     *
     * This rule traverses bottom-up from the original Spark plan and for each plan node, there
     * are a few cases to consider:
     *
     * 1. The child(ren) of the current node `p` cannot be converted to native
     *   In this case, we'll simply return the original Spark plan, since Comet native
     *   execution cannot start from an arbitrary Spark operator (unless it is special node
     *   such as scan or sink such as union etc., which are wrapped by
     *   `CometScanWrapper` and `CometSinkPlaceHolder` respectively).
     *
     * 2. The child(ren) of the current node `p` can be converted to native
     *   There are two sub-cases for this scenario: 1) This node `p` can also be converted to
     *   native. In this case, we'll create a new native Comet operator for `p` and connect it with
     *   its previously converted child(ren); 2) This node `p` cannot be converted to native. In
     *   this case, similar to 1) above, we simply return `p` as it is. Its child(ren) would still
     *   be native Comet operators.
     *
     * After this rule finishes, we'll do another pass on the final plan to convert all adjacent
     * Comet native operators into a single native execution block. Please see where
     * `convertBlock` is called below.
     *
     * Here are a few examples:
     *
     *     Scan                       ======>             CometScan
     *      |                                                |
     *     Filter                                         CometFilter
     *      |                                                |
     *     HashAggregate                                  CometHashAggregate
     *      |                                                |
     *     Exchange                                       CometExchange
     *      |                                                |
     *     HashAggregate                                  CometHashAggregate
     *      |                                                |
     *     UnsupportedOperator                            UnsupportedOperator
     *
     * Native execution doesn't necessarily have to start from `CometScan`:
     *
     *     Scan                       =======>            CometScan
     *      |                                                |
     *     UnsupportedOperator                            UnsupportedOperator
     *      |                                                |
     *     HashAggregate                                  HashAggregate
     *      |                                                |
     *     Exchange                                       CometExchange
     *      |                                                |
     *     HashAggregate                                  CometHashAggregate
     *      |                                                |
     *     UnsupportedOperator                            UnsupportedOperator
     *
     * A sink can also be Comet operators other than `CometExchange`, for instance `CometUnion`:
     *
     *     Scan   Scan                =======>          CometScan CometScan
     *      |      |                                       |         |
     *     Filter Filter                                CometFilter CometFilter
     *      |      |                                       |         |
     *        Union                                         CometUnion
     *          |                                               |
     *        Project                                       CometProject
     */
    // spotless:on
    private def transform(plan: SparkPlan): SparkPlan = {
      def transform1(op: UnaryExecNode): Option[Operator] = {
        op.child match {
          case childNativeOp: CometNativeExec =>
            QueryPlanSerde.operator2Proto(op, childNativeOp.nativeOp)
          case _ =>
            None
        }
      }

      plan.transformUp {
        case op if isCometScan(op) =>
          val nativeOp = QueryPlanSerde.operator2Proto(op).get
          CometScanWrapper(nativeOp, op)

        case op: ProjectExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometProjectExec(nativeOp, op, op.projectList, op.output, op.child)
            case None =>
              op
          }

        case op: FilterExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometFilterExec(nativeOp, op, op.condition, op.child)
            case None =>
              op
          }

        case op: SortExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometSortExec(nativeOp, op, op.sortOrder, op.child)
            case None =>
              op
          }

        case op: LocalLimitExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometLocalLimitExec(nativeOp, op, op.limit, op.child)
            case None =>
              op
          }

        case op: GlobalLimitExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometGlobalLimitExec(nativeOp, op, op.limit, op.child)
            case None =>
              op
          }

        case op: ExpandExec =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              CometExpandExec(nativeOp, op, op.projections, op.child)
            case None =>
              op
          }

        case op @ HashAggregateExec(_, _, _, groupingExprs, aggExprs, _, _, _, child) =>
          val newOp = transform1(op)
          newOp match {
            case Some(nativeOp) =>
              val modes = aggExprs.map(_.mode).distinct
              assert(modes.length == 1)
              CometHashAggregateExec(
                nativeOp,
                op,
                groupingExprs,
                aggExprs,
                child.output,
                modes.head,
                child)
            case None =>
              op
          }

        case c @ CoalesceExec(numPartitions, child)
            if isCometOperatorEnabled(conf, "coalesce")
              && isCometNative(child) =>
          QueryPlanSerde.operator2Proto(c) match {
            case Some(nativeOp) =>
              val cometOp = CometCoalesceExec(c, numPartitions, child)
              CometSinkPlaceHolder(nativeOp, c, cometOp)
            case None =>
              c
          }

        case u: UnionExec
            if isCometOperatorEnabled(conf, "union") &&
              u.children.forall(isCometNative) =>
          QueryPlanSerde.operator2Proto(u) match {
            case Some(nativeOp) =>
              val cometOp = CometUnionExec(u, u.children)
              CometSinkPlaceHolder(nativeOp, u, cometOp)
          }

        case op =>
          // An operator that is not supported by Comet
          op
      }
    }

    override def apply(plan: SparkPlan): SparkPlan = {
      // DataFusion doesn't have ANSI mode. For now we just disable CometExec if ANSI mode is
      // enabled.
      if (isANSIEnabled(conf)) {
        logInfo("Comet extension disabled for ANSI mode")
        return plan
      }

      // We shouldn't transform Spark query plan if Comet is disabled.
      if (!isCometEnabled(conf)) return plan

      if (!isCometExecEnabled(conf)) {
        // Comet exec is disabled
        plan
      } else {
        var newPlan = transform(plan)

        // Remove placeholders
        newPlan = newPlan.transform {
          case CometSinkPlaceHolder(_, _, s) => s
          case CometScanWrapper(_, s) => s
        }

        // Set up logical links
        newPlan = newPlan.transform { case op: CometExec =>
          op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          op
        }

        // Convert native execution block by linking consecutive native operators.
        var firstNativeOp = true
        newPlan.transformDown {
          case op: CometNativeExec =>
            if (firstNativeOp) {
              op.convertBlock()
              firstNativeOp = false
            }
            op
          case op =>
            firstNativeOp = true
            op
        }
      }
    }
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)

  private[comet] def isANSIEnabled(conf: SQLConf): Boolean = {
    conf.getConf(SQLConf.ANSI_ENABLED)
  }

  /**
   * Checks whether Comet extension should be enabled for Spark.
   */
  private[comet] def isCometEnabled(conf: SQLConf): Boolean = {
    if (isBigEndian) {
      logInfo("Comet extension is disabled because platform is big-endian")
      return false
    }
    if (!COMET_ENABLED.get(conf)) {
      logInfo(s"Comet extension is disabled, please turn on s${COMET_ENABLED.key} to enable it")
      return false
    }

    // We don't support INT96 timestamps written by Apache Impala in a different timezone yet
    if (conf.getConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION)) {
      logWarning(
        "Comet extension is disabled, because it currently doesn't support" +
          s" ${SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION} setting to true.")
      return false
    }

    try {
      // This will load the Comet native lib on demand, and if success, should set
      // `NativeBase.loaded` to true
      NativeBase.isLoaded
    } catch {
      case e: Throwable =>
        if (COMET_NATIVE_LOAD_REQUIRED.get(conf)) {
          throw new CometRuntimeException(
            "Error when loading native library. Please fix the error and try again, or fallback " +
              s"to Spark by setting ${COMET_ENABLED.key} to false",
            e)
        } else {
          logWarning(
            "Comet extension is disabled because of error when loading native lib. " +
              "Falling back to Spark",
            e)
        }
        false
    }
  }

  private[comet] def isCometOperatorEnabled(conf: SQLConf, operator: String): Boolean = {
    val operatorFlag = s"$COMET_EXEC_CONFIG_PREFIX.$operator.enabled"
    conf.getConfString(operatorFlag, "false").toBoolean || isCometAllOperatorEnabled(conf)
  }

  private[comet] def isCometScanEnabled(conf: SQLConf): Boolean = {
    COMET_SCAN_ENABLED.get(conf)
  }

  private[comet] def isCometExecEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ENABLED.get(conf)
  }

  private[comet] def isCometAllOperatorEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ALL_OPERATOR_ENABLED.get(conf)
  }

  private[comet] def isCometAllExprEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ALL_EXPR_ENABLED.get(conf)
  }

  private[comet] def isSchemaSupported(schema: StructType): Boolean =
    schema.map(_.dataType).forall(isTypeSupported)

  private[comet] def isTypeSupported(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        BinaryType | StringType | _: DecimalType | DateType | TimestampType =>
      true
    // `TimestampNTZType` is private in Spark 3.2.
    case t: DataType if t.typeName == "timestamp_ntz" && !isSpark32 => true
    case dt =>
      logInfo(s"Comet extension is disabled because data type $dt is not supported")
      false
  }

  def isCometScan(op: SparkPlan): Boolean = {
    op.isInstanceOf[CometBatchScanExec] || op.isInstanceOf[CometScanExec]
  }

  /** Used for operations that weren't available in Spark 3.2 */
  def isSpark32: Boolean = {
    org.apache.spark.SPARK_VERSION.matches("3\\.2\\..*")
  }

  /** Used for operations that are available in Spark 3.4+ */
  def isSpark34Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.4"
  }

  /** Calculates required memory overhead in MB per executor process for Comet. */
  def getCometMemoryOverheadInMiB(sparkConf: SparkConf): Long = {
    // `spark.executor.memory` default value is 1g
    val executorMemoryMiB = ConfigHelpers
      .byteFromString(sparkConf.get("spark.executor.memory", "1024MB"), ByteUnit.MiB)

    val minimum = ConfigHelpers
      .byteFromString(sparkConf.get(COMET_MEMORY_OVERHEAD_MIN_MIB.key, "384"), ByteUnit.MiB)
    val overheadFactor = sparkConf.getDouble(COMET_MEMORY_OVERHEAD_FACTOR.key, 0.2)

    val overHeadMemFromConf = sparkConf
      .getOption(COMET_MEMORY_OVERHEAD.key)
      .map(ConfigHelpers.byteFromString(_, ByteUnit.MiB))

    overHeadMemFromConf.getOrElse(math.max((overheadFactor * executorMemoryMiB).toLong, minimum))
  }

  /** Calculates required memory overhead in bytes per executor process for Comet. */
  def getCometMemoryOverhead(sparkConf: SparkConf): Long = {
    ByteUnit.MiB.toBytes(getCometMemoryOverheadInMiB(sparkConf))
  }
}
