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

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.rules.{CometExecRule, CometScanRule, EliminateRedundantTransitions}
import org.apache.comet.shims.ShimCometSparkSessionExtensions

/**
 * CometDriverPlugin will register an instance of this class with Spark.
 *
 * This class is responsible for injecting Comet rules and extensions into Spark.
 */
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

    override def postColumnarTransitions: Rule[SparkPlan] =
      EliminateRedundantTransitions(session)
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)

  private[comet] def isANSIEnabled(conf: SQLConf): Boolean = {
    conf.getConf(SQLConf.ANSI_ENABLED)
  }

  /**
   * Checks whether Comet extension should be loaded for Spark.
   */
  private[comet] def isCometLoaded(conf: SQLConf): Boolean = {
    if (isBigEndian) {
      logInfo("Comet extension is disabled because platform is big-endian")
      return false
    }
    if (!COMET_ENABLED.get(conf)) {
      logInfo(s"Comet extension is disabled, please turn on ${COMET_ENABLED.key} to enable it")
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

  private[comet] def isCometBroadCastForceEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_BROADCAST_FORCE_ENABLED.get(conf)
  }

  private[comet] def getCometBroadcastNotEnabledReason(conf: SQLConf): Option[String] = {
    if (!CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.get(conf) &&
      !isCometBroadCastForceEnabled(conf)) {
      Some(
        s"${COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key}.enabled is not specified and " +
          s"${COMET_EXEC_BROADCAST_FORCE_ENABLED.key} is not specified")
    } else {
      None
    }
  }

  // Check whether Comet shuffle is enabled:
  // 1. `COMET_EXEC_SHUFFLE_ENABLED` is true
  // 2. `spark.shuffle.manager` is set to `CometShuffleManager`
  // 3. Off-heap memory is enabled || Spark/Comet unit testing
  private[comet] def isCometShuffleEnabled(conf: SQLConf): Boolean =
    COMET_EXEC_SHUFFLE_ENABLED.get(conf) && isCometShuffleManagerEnabled(conf)

  private[comet] def getCometShuffleNotEnabledReason(conf: SQLConf): Option[String] = {
    if (!COMET_EXEC_SHUFFLE_ENABLED.get(conf)) {
      Some(s"${COMET_EXEC_SHUFFLE_ENABLED.key} is not enabled")
    } else if (!isCometShuffleManagerEnabled(conf)) {
      Some(s"spark.shuffle.manager is not set to ${CometShuffleManager.getClass.getName}")
    } else {
      None
    }
  }

  private def isCometShuffleManagerEnabled(conf: SQLConf) = {
    conf.contains("spark.shuffle.manager") && conf.getConfString("spark.shuffle.manager") ==
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
  }

  private[comet] def isCometScanEnabled(conf: SQLConf): Boolean = {
    COMET_NATIVE_SCAN_ENABLED.get(conf)
  }

  private[comet] def isCometExecEnabled(conf: SQLConf): Boolean = {
    COMET_EXEC_ENABLED.get(conf)
  }

  private[comet] def isCometNativeShuffleMode(conf: SQLConf): Boolean = {
    COMET_SHUFFLE_MODE.get(conf) match {
      case "native" => true
      case "auto" => true
      case _ => false
    }
  }

  private[comet] def isCometJVMShuffleMode(conf: SQLConf): Boolean = {
    COMET_SHUFFLE_MODE.get(conf) match {
      case "jvm" => true
      case "auto" => true
      case _ => false
    }
  }

  def isCometScan(op: SparkPlan): Boolean = {
    op.isInstanceOf[CometBatchScanExec] || op.isInstanceOf[CometScanExec]
  }

  def shouldApplySparkToColumnar(conf: SQLConf, op: SparkPlan): Boolean = {
    // Only consider converting leaf nodes to columnar currently, so that all the following
    // operators can have a chance to be converted to columnar. Leaf operators that output
    // columnar batches, such as Spark's vectorized readers, will also be converted to native
    // comet batches.
    val fallbackReasons = new ListBuffer[String]()
    if (CometSparkToColumnarExec.isSchemaSupported(op.schema, fallbackReasons)) {
      op match {
        // Convert Spark DS v1 scan to Arrow format
        case scan: FileSourceScanExec =>
          scan.relation.fileFormat match {
            case _: CSVFileFormat => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonFileFormat => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetFileFormat => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // Convert Spark DS v2 scan to Arrow format
        case scan: BatchScanExec =>
          scan.scan match {
            case _: CSVScan => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonScan => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetScan => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // other leaf nodes
        case _: LeafExecNode =>
          isSparkToArrowEnabled(conf, op)
        case _ =>
          // TODO: consider converting other intermediate operators to columnar.
          false
      }
    } else {
      false
    }
  }

  private def isSparkToArrowEnabled(conf: SQLConf, op: SparkPlan) = {
    COMET_SPARK_TO_ARROW_ENABLED.get(conf) && {
      val simpleClassName = Utils.getSimpleName(op.getClass)
      val nodeName = simpleClassName.replaceAll("Exec$", "")
      COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.get(conf).contains(nodeName)
    }
  }

  def isSpark35Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.5"
  }

  def isSpark40Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.0"
  }

  def usingDataSourceExec(conf: SQLConf): Boolean =
    Seq(CometConf.SCAN_NATIVE_ICEBERG_COMPAT, CometConf.SCAN_NATIVE_DATAFUSION).contains(
      CometConf.COMET_NATIVE_SCAN_IMPL.get(conf))

  def usingDataSourceExecWithIncompatTypes(conf: SQLConf): Boolean = {
    usingDataSourceExec(conf) &&
    !CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.get(conf)
  }

  /**
   * Whether we should override Spark memory configuration for Comet. This only returns true when
   * Comet native execution is enabled and/or Comet shuffle is enabled and Comet doesn't use
   * off-heap mode (unified memory manager).
   */
  def shouldOverrideMemoryConf(conf: SparkConf): Boolean = {
    val cometEnabled = getBooleanConf(conf, CometConf.COMET_ENABLED)
    val cometShuffleEnabled = getBooleanConf(conf, CometConf.COMET_EXEC_SHUFFLE_ENABLED)
    val cometExecEnabled = getBooleanConf(conf, CometConf.COMET_EXEC_ENABLED)
    val offHeapMode = CometSparkSessionExtensions.isOffHeapEnabled(conf)
    cometEnabled && (cometShuffleEnabled || cometExecEnabled) && !offHeapMode
  }

  /**
   * Calculates required memory overhead in MB per executor process for Comet when running in
   * on-heap mode.
   *
   * If `COMET_MEMORY_OVERHEAD` is defined then that value will be used, otherwise the overhead
   * will be calculated by multiplying executor memory (`spark.executor.memory`) by
   * `COMET_MEMORY_OVERHEAD_FACTOR`.
   *
   * In either case, a minimum value of `COMET_MEMORY_OVERHEAD_MIN_MIB` will be returned.
   */
  def getCometMemoryOverheadInMiB(sparkConf: SparkConf): Long = {
    if (isOffHeapEnabled(sparkConf)) {
      // when running in off-heap mode we use unified memory management to share
      // off-heap memory with Spark so do not add overhead
      return 0
    }

    // `spark.executor.memory` default value is 1g
    val baseMemoryMiB = ConfigHelpers
      .byteFromString(sparkConf.get("spark.executor.memory", "1024MB"), ByteUnit.MiB)

    val cometMemoryOverheadMinAsString = sparkConf.get(
      COMET_MEMORY_OVERHEAD_MIN_MIB.key,
      COMET_MEMORY_OVERHEAD_MIN_MIB.defaultValueString)

    val minimum = ConfigHelpers.byteFromString(cometMemoryOverheadMinAsString, ByteUnit.MiB)
    val overheadFactor = getDoubleConf(sparkConf, COMET_MEMORY_OVERHEAD_FACTOR)

    val overHeadMemFromConf = sparkConf
      .getOption(COMET_MEMORY_OVERHEAD.key)
      .map(ConfigHelpers.byteFromString(_, ByteUnit.MiB))

    overHeadMemFromConf.getOrElse(math.max((overheadFactor * baseMemoryMiB).toLong, minimum))
  }

  private def getBooleanConf(conf: SparkConf, entry: ConfigEntry[Boolean]) =
    conf.getBoolean(entry.key, entry.defaultValue.get)

  private def getDoubleConf(conf: SparkConf, entry: ConfigEntry[Double]) =
    conf.getDouble(entry.key, entry.defaultValue.get)

  /**
   * Calculates required memory overhead in bytes per executor process for Comet when running in
   * on-heap mode.
   */
  def getCometMemoryOverhead(sparkConf: SparkConf): Long = {
    ByteUnit.MiB.toBytes(getCometMemoryOverheadInMiB(sparkConf))
  }

  /**
   * Calculates required shuffle memory size in bytes per executor process for Comet when running
   * in on-heap mode.
   */
  def getCometShuffleMemorySize(sparkConf: SparkConf, conf: SQLConf = SQLConf.get): Long = {
    assert(!isOffHeapEnabled(sparkConf))

    val cometMemoryOverhead = getCometMemoryOverheadInMiB(sparkConf)

    val overheadFactor = COMET_COLUMNAR_SHUFFLE_MEMORY_FACTOR.get(conf)
    val cometShuffleMemoryFromConf = COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.get(conf)

    val shuffleMemorySize =
      cometShuffleMemoryFromConf.getOrElse((overheadFactor * cometMemoryOverhead).toLong)
    if (shuffleMemorySize > cometMemoryOverhead) {
      logWarning(
        s"Configured shuffle memory size $shuffleMemorySize is larger than Comet memory overhead " +
          s"$cometMemoryOverhead, using Comet memory overhead instead.")
      ByteUnit.MiB.toBytes(cometMemoryOverhead)
    } else {
      ByteUnit.MiB.toBytes(shuffleMemorySize)
    }
  }

  def isOffHeapEnabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean("spark.memory.offHeap.enabled", false)
  }

  /**
   * Attaches explain information to a TreeNode, rolling up the corresponding information tags
   * from any child nodes. For now, we are using this to attach the reasons why certain Spark
   * operators or expressions are disabled.
   *
   * @param node
   *   The node to attach the explain information to. Typically a SparkPlan
   * @param info
   *   Information text. Optional, may be null or empty. If not provided, then only information
   *   from child nodes will be included.
   * @param exprs
   *   Child nodes. Information attached in these nodes will be be included in the information
   *   attached to @node
   * @tparam T
   *   The type of the TreeNode. Typically SparkPlan, AggregateExpression, or Expression
   * @return
   *   The node with information (if any) attached
   */
  def withInfo[T <: TreeNode[_]](node: T, info: String, exprs: T*): T = {
    // support existing approach of passing in multiple infos in a newline-delimited string
    val infoSet = if (info == null || info.isEmpty) {
      Set.empty[String]
    } else {
      info.split("\n").toSet
    }
    withInfos(node, infoSet, exprs: _*)
  }

  /**
   * Attaches explain information to a TreeNode, rolling up the corresponding information tags
   * from any child nodes. For now, we are using this to attach the reasons why certain Spark
   * operators or expressions are disabled.
   *
   * @param node
   *   The node to attach the explain information to. Typically a SparkPlan
   * @param info
   *   Information text. May contain zero or more strings. If not provided, then only information
   *   from child nodes will be included.
   * @param exprs
   *   Child nodes. Information attached in these nodes will be be included in the information
   *   attached to @node
   * @tparam T
   *   The type of the TreeNode. Typically SparkPlan, AggregateExpression, or Expression
   * @return
   *   The node with information (if any) attached
   */
  def withInfos[T <: TreeNode[_]](node: T, info: Set[String], exprs: T*): T = {
    val existingNodeInfos = node.getTagValue(CometExplainInfo.EXTENSION_INFO)
    val newNodeInfo = (existingNodeInfos ++ exprs
      .flatMap(_.getTagValue(CometExplainInfo.EXTENSION_INFO))).flatten.toSet
    node.setTagValue(CometExplainInfo.EXTENSION_INFO, newNodeInfo ++ info)
    node
  }

  /**
   * Attaches explain information to a TreeNode, rolling up the corresponding information tags
   * from any child nodes
   *
   * @param node
   *   The node to attach the explain information to. Typically a SparkPlan
   * @param exprs
   *   Child nodes. Information attached in these nodes will be be included in the information
   *   attached to @node
   * @tparam T
   *   The type of the TreeNode. Typically SparkPlan, AggregateExpression, or Expression
   * @return
   *   The node with information (if any) attached
   */
  def withInfo[T <: TreeNode[_]](node: T, exprs: T*): T = {
    withInfos(node, Set.empty, exprs: _*)
  }

  // Helper to reduce boilerplate
  def createMessage(condition: Boolean, message: => String): Option[String] = {
    if (condition) {
      Some(message)
    } else {
      None
    }
  }
}
