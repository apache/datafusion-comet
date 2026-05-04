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

package org.apache.comet.planner

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometCsvNativeScanExec, CometIcebergNativeScanExec, CometNativeExec, CometScanWrapper, CometSinkPlaceHolder, CometSparkToColumnarExec, SerializedPlan}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}

import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.planner.tags.CometTags
import org.apache.comet.planner.tags.PlannerDecision.{Convert, ConvertS2C}
import org.apache.comet.rules.CometExecRule
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass}
import org.apache.comet.serde.operator.{partition2Proto, schema2Proto, CometExchangeSink, CometNativeScan}

/**
 * Phase 3: bottom-up emit. For each node tagged `DECISION=Convert`, call the matching serde's
 * `convert` to build the protobuf, then `createExec` to produce the CometNativeExec. Wires
 * children via the protobuf children list. Sets the `COMET_CONVERTED` tag so AQE re-entries can
 * short-circuit.
 *
 * Handles:
 *   - V1 FileSourceScanExec (native_datafusion path) via CometNativeScan serde directly.
 *   - V2 BatchScanExec: Iceberg (metadata pre-stashed on ICEBERG_METADATA tag by Phase 2) and CSV
 *     (scan.scan is a CSVScan). Neither goes through a CometBatchScanExec wrapper. Phase 3 builds
 *     protobuf and the final CometIcebergNativeScanExec / CometCsvNativeScanExec directly from
 *     the raw BatchScanExec.
 *   - Spark-to-columnar leaves (DECISION=ConvertS2C): wrap via CometSparkToColumnarExec serde.
 *   - ShuffleExchangeExec via CometShuffleExchangeExec serde.
 *   - BroadcastExchangeExec via CometBroadcastExchangeExec serde (when children native).
 *   - Generic exec operators whose children are already CometNativeExec, via the allExecs map.
 */
case class Phase3Emit(session: SparkSession) extends Logging {

  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case op if decidedS2C(op) =>
        val out = emitS2C(op).getOrElse(op)
        logEmit("S2C", op, out)
        out

      case scan: FileSourceScanExec if decidedToConvert(scan) =>
        val out = emitV1NativeScan(scan).getOrElse(scan)
        logEmit("V1Native", scan, out)
        out

      case scan: BatchScanExec if decidedToConvert(scan) =>
        val out = emitV2Scan(scan).getOrElse(scan)
        logEmit("V2Native", scan, out)
        out

      case s: ShuffleExchangeExec if decidedToConvert(s) =>
        val out = emitShuffle(s).getOrElse(s)
        logEmit("Shuffle", s, out)
        out

      // AQE stage already wrapping a Comet exchange from a prior pass. Wrap as a Comet sink so
      // the parent's protobuf wiring treats this stage as native-compatible.
      case s: ShuffleQueryStageExec if decidedToConvert(s) && isCometShuffleStage(s) =>
        val out = emitExchangeSink(s).getOrElse(s)
        logEmit("ShuffleStage", s, out)
        out

      case b: BroadcastQueryStageExec if decidedToConvert(b) && isCometBroadcastStage(b) =>
        val out = emitExchangeSink(b).getOrElse(b)
        logEmit("BroadcastStage", b, out)
        out

      case b: BroadcastExchangeExec if decidedToConvert(b) && allChildrenNative(b) =>
        val out = emitBroadcast(b).getOrElse(b)
        logEmit("Broadcast", b, out)
        out

      case op if decidedToConvert(op) && allChildrenNative(op) =>
        val out = lookupSerde(op).flatMap(serde => emitGeneric(op, serde)).getOrElse(op)
        logEmit("Generic", op, out)
        out

      case op =>
        if (decidedToConvert(op)) {
          logWarning(
            s"Phase3: DECISION=Convert but no emitter matched node=${op.getClass.getSimpleName} " +
              s"id=${op.id} allChildrenNative=${allChildrenNative(op)}")
        }
        op
    }
  }

  private def decidedToConvert(op: SparkPlan): Boolean =
    op.getTagValue(CometTags.DECISION).contains(Convert)

  private def decidedS2C(op: SparkPlan): Boolean =
    op.getTagValue(CometTags.DECISION).contains(ConvertS2C)

  private def isCometShuffleStage(s: ShuffleQueryStageExec): Boolean = s.plan match {
    case _: CometShuffleExchangeExec => true
    case ReusedExchangeExec(_, _: CometShuffleExchangeExec) => true
    case _ => false
  }

  private def isCometBroadcastStage(b: BroadcastQueryStageExec): Boolean = b.plan match {
    case _: CometBroadcastExchangeExec => true
    case ReusedExchangeExec(_, _: CometBroadcastExchangeExec) => true
    case _ => false
  }

  /**
   * Emit a stage-wrapped Comet exchange (`ShuffleQueryStageExec` / `BroadcastQueryStageExec`
   * holding a `CometShuffleExchangeExec` / `CometBroadcastExchangeExec` from a prior pass) as a
   * Comet sink. Uses `CometExchangeSink` default `convert` which builds a Scan operator, and the
   * returned wrapper is unwrapped in `runSerde`, leaving the stage with a `NATIVE_OP` tag.
   */
  private def emitExchangeSink(op: SparkPlan): Option[SparkPlan] =
    runSerde(op, CometExchangeSink, childOps = Seq.empty)

  /**
   * A child counts as native-compatible for protobuf wiring if it is itself a CometNativeExec or
   * carries a `NATIVE_OP` tag. The tag is set by `runSerde` on JVM-orchestrated operators
   * (`CometCollectLimitExec`, `CometBroadcastExchangeExec`, `CometSparkToColumnarExec`,
   * `CometUnionExec`, `CometTakeOrderedAndProjectExec`, `CometCoalesceExec`) where the serde
   * previously wrapped them in a placeholder. Those wrappers no longer appear in the plan tree.
   */
  private def isNativeCompatible(child: SparkPlan): Boolean =
    child.isInstanceOf[CometNativeExec] || child.getTagValue(CometTags.NATIVE_OP).isDefined

  // Vacuous truth for leaf nodes (children.isEmpty): a leaf has no children to fail the predicate,
  // so the generic emit path can wire it via its serde with no child nativeOps. Without this,
  // leaves like LocalTableScanExec / RangeExec / InMemoryTableScanExec slip past the generic
  // case and never get converted.
  private def allChildrenNative(op: SparkPlan): Boolean =
    op.children.forall(isNativeCompatible)

  /**
   * Pulls the protobuf operator representing a child for parent wiring. Reads `nativeOp` from a
   * `CometNativeExec` child, falls back to the `NATIVE_OP` tag for JVM-orchestrated children.
   */
  private def nativeOpOf(child: SparkPlan): OperatorOuterClass.Operator = child match {
    case n: CometNativeExec => n.nativeOp
    case other =>
      other
        .getTagValue(CometTags.NATIVE_OP)
        .getOrElse(
          throw new IllegalStateException(
            s"Child not native-compatible. class=${other.getClass.getSimpleName}"))
  }

  private def lookupSerde(op: SparkPlan): Option[CometOperatorSerde[SparkPlan]] =
    CometExecRule.allExecs
      .get(op.getClass)
      .map(_.asInstanceOf[CometOperatorSerde[SparkPlan]])

  private def emitGeneric(
      op: SparkPlan,
      serde: CometOperatorSerde[SparkPlan]): Option[SparkPlan] = {
    assert(
      allChildrenNative(op),
      s"emitGeneric invoked with non-native children node=${op.getClass.getSimpleName} id=${op.id}")
    runSerde(op, serde, op.children.map(nativeOpOf))
  }

  /**
   * Emit a V1 native scan directly from the FileSourceScanExec, no intermediate CometScanExec
   * wrapping. The serde operates on FileSourceScanExec after the type-param refactor.
   */
  private def emitV1NativeScan(scanExec: FileSourceScanExec): Option[SparkPlan] = {
    assert(
      decidedToConvert(scanExec),
      s"emitV1NativeScan invoked without DECISION=Convert scan=${scanExec.id}")
    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(scanExec.id)
    val nativeOpOpt = CometNativeScan.convert(scanExec, builder)
    if (nativeOpOpt.isEmpty) {
      logDebug(s"Phase3: V1 serde.convert returned None scan=${scanExec.id}")
    }
    nativeOpOpt.map { nativeOp =>
      val exec = CometNativeScan.createExec(nativeOp, scanExec)
      scanExec.logicalLink.foreach(exec.setLogicalLink)
      exec.setTagValue(CometTags.COMET_CONVERTED, ())
      exec
    }
  }

  /**
   * Emit a CometSparkToColumnarExec-wrapped node for a leaf that Phase 2 decided should bridge
   * row-at-a-time Spark data into a Comet-consuming parent. The existing `CometSink` default
   * `convert` builds a Scan operator with the node's schema. `createExec` wraps the leaf in
   * `CometScanWrapper(nativeOp, CometSparkToColumnarExec(op))`. The CometScanWrapper indirection
   * goes away when the old rule is deleted.
   */
  private def emitS2C(op: SparkPlan): Option[SparkPlan] = {
    assert(decidedS2C(op), s"emitS2C invoked without DECISION=ConvertS2C node=${op.id}")
    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
    val nativeOpOpt = CometSparkToColumnarExec.convert(op, builder)
    if (nativeOpOpt.isEmpty) {
      logDebug(s"Phase3: S2C serde.convert returned None node=${op.id}")
    }
    nativeOpOpt.map { nativeOp =>
      val raw = CometSparkToColumnarExec.createExec(nativeOp, op)
      val exec = raw match {
        case CometScanWrapper(_, inner) =>
          inner.setTagValue(CometTags.NATIVE_OP, nativeOp)
          inner
        case direct => direct
      }
      op.logicalLink.foreach(exec.setLogicalLink)
      exec.setTagValue(CometTags.COMET_CONVERTED, ())
      exec
    }
  }

  /**
   * Emit a V2 native scan directly from the BatchScanExec. Dispatch: if Phase 2 stashed iceberg
   * metadata on the `ICEBERG_METADATA` tag, emit a CometIcebergNativeScanExec. Otherwise if the
   * scan is a CSVScan, emit a CometCsvNativeScanExec. Any other scan falls through. Neither path
   * goes through a CometBatchScanExec wrapper.
   */
  private def emitV2Scan(scanExec: BatchScanExec): Option[SparkPlan] = {
    assert(
      decidedToConvert(scanExec),
      s"emitV2Scan invoked without DECISION=Convert scan=${scanExec.id}")
    scanExec.getTagValue(CometTags.ICEBERG_METADATA) match {
      case Some(metadata) =>
        val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(scanExec.id)
        val icebergScanBuilder = OperatorOuterClass.IcebergScan.newBuilder()
        val commonBuilder = OperatorOuterClass.IcebergScanCommon.newBuilder()
        // Only metadata_location is needed at planning time. catalog_properties,
        // required_schema, pools and per-partition data are populated by
        // CometIcebergNativeScan.serializePartitions at execution time after DPP resolves.
        commonBuilder.setMetadataLocation(metadata.metadataLocation)
        icebergScanBuilder.setCommon(commonBuilder.build())
        builder.clearChildren()
        val nativeOp = builder.setIcebergScan(icebergScanBuilder).build()
        val exec = CometIcebergNativeScanExec(
          nativeOp,
          scanExec,
          session,
          metadata.metadataLocation,
          metadata)
        exec.setTagValue(CometTags.COMET_CONVERTED, ())
        // TreeNode._tags is not @transient, so a lingering metadata tag on the raw
        // BatchScanExec could serialize if anything walked a mid-planner plan. Drop it so the
        // heavy Iceberg fields never reach a serialized form of the original BatchScanExec.
        scanExec.unsetTagValue(CometTags.ICEBERG_METADATA)
        assert(
          scanExec.getTagValue(CometTags.ICEBERG_METADATA).isEmpty,
          s"ICEBERG_METADATA tag still present after unset scan=${scanExec.id}")
        Some(exec)

      case None =>
        scanExec.scan match {
          case csvScan: CSVScan =>
            Some(emitCsvScan(scanExec, csvScan))
          case other =>
            logWarning(
              s"Phase3: V2 decided Convert but no emit path scan=${scanExec.id} " +
                s"scanClass=${other.getClass.getName}")
            None
        }
    }
  }

  private def emitCsvScan(scanExec: BatchScanExec, csvScan: CSVScan): SparkPlan = {
    val sessionState = session.sessionState
    val options = {
      val columnPruning = sessionState.conf.csvColumnPruning
      val timeZone = sessionState.conf.sessionLocalTimeZone
      new CSVOptions(csvScan.options.asScala.toMap, columnPruning, timeZone)
    }
    val filePartitions = scanExec.inputPartitions.map(_.asInstanceOf[FilePartition])
    val csvOptionsProto = csvOptions2Proto(options)
    val dataSchemaProto = schema2Proto(csvScan.dataSchema.fields)
    val readSchemaFieldNames = csvScan.readDataSchema.fieldNames
    val projectionVector = csvScan.dataSchema.fields.zipWithIndex
      .filter { case (field, _) => readSchemaFieldNames.contains(field.name) }
      .map(_._2.asInstanceOf[Integer])
    val partitionSchemaProto = schema2Proto(csvScan.readPartitionSchema.fields)
    val partitionsProto = filePartitions.map(partition2Proto(_, csvScan.readPartitionSchema))
    val objectStoreOptions = filePartitions.headOption
      .flatMap { partitionFile =>
        val hadoopConf = sessionState
          .newHadoopConfWithOptions(session.sparkContext.getConf.getAll.toMap)
        partitionFile.files.headOption
          .map(file => NativeConfig.extractObjectStoreOptions(hadoopConf, file.pathUri))
      }
      .getOrElse(Map.empty)

    val csvScanBuilder = OperatorOuterClass.CsvScan.newBuilder()
    csvScanBuilder.putAllObjectStoreOptions(objectStoreOptions.asJava)
    csvScanBuilder.setCsvOptions(csvOptionsProto)
    csvScanBuilder.addAllFilePartitions(partitionsProto.asJava)
    csvScanBuilder.addAllDataSchema(dataSchemaProto.toIterable.asJava)
    csvScanBuilder.addAllProjectionVector(projectionVector.toIterable.asJava)
    csvScanBuilder.addAllPartitionSchema(partitionSchemaProto.toIterable.asJava)

    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(scanExec.id)
    val nativeOp = builder.setCsvScan(csvScanBuilder).build()

    val exec =
      CometCsvNativeScanExec(nativeOp, scanExec.output, scanExec, SerializedPlan(None))
    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec.setTagValue(CometTags.COMET_CONVERTED, ())
    exec
  }

  private def csvOptions2Proto(options: CSVOptions): OperatorOuterClass.CsvOptions = {
    val csvOptionsBuilder = OperatorOuterClass.CsvOptions.newBuilder()
    csvOptionsBuilder.setDelimiter(options.delimiter)
    csvOptionsBuilder.setHasHeader(options.headerFlag)
    csvOptionsBuilder.setQuote(options.quote.toString)
    csvOptionsBuilder.setEscape(options.escape.toString)
    csvOptionsBuilder.setTerminator(options.lineSeparator.getOrElse("\n"))
    csvOptionsBuilder.setTruncatedRows(options.multiLine)
    if (options.isCommentSet) {
      csvOptionsBuilder.setComment(options.comment.toString)
    }
    csvOptionsBuilder.build()
  }

  /**
   * Emit a shuffle without going through `CometShuffleExchangeExec.createExec`. That serde guards
   * the native-shuffle path with `op.children.forall(_.isInstanceOf[CometNativeExec])`, which is
   * too strict after we unwrapped placeholder-based children to carry `NATIVE_OP` tags instead.
   * Phase 3 takes the same branches the serde would (native vs columnar) and constructs
   * `CometShuffleExchangeExec` directly. Protobuf comes from `serde.convert`.
   */
  private def emitShuffle(s: ShuffleExchangeExec): Option[SparkPlan] = {
    val supportOpt = CometShuffleExchangeExec.shuffleSupported(s)
    if (supportOpt.isEmpty) {
      logDebug(s"Phase3: shuffle serde rejected at emit time id=${s.id}")
      return None
    }
    val shuffleType = supportOpt.get
    val childOps = s.children.flatMap { c =>
      if (isNativeCompatible(c)) Some(nativeOpOf(c)) else None
    }
    if (childOps.size != s.children.size) {
      logDebug(
        s"Phase3: shuffle has mixed Comet / Spark children id=${s.id} " +
          s"cometChildren=${childOps.size} totalChildren=${s.children.size}")
    }
    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(s.id)
    childOps.foreach(builder.addChildren)
    CometShuffleExchangeExec.convert(s, builder, childOps: _*).map { nativeOp =>
      val exec = shuffleType match {
        case CometNativeShuffle => CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
        case CometColumnarShuffle =>
          CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
      }
      exec.setTagValue(CometTags.NATIVE_OP, nativeOp)
      s.logicalLink.foreach(exec.setLogicalLink)
      exec.setTagValue(CometTags.COMET_CONVERTED, ())
      exec
    }
  }

  private def emitBroadcast(b: BroadcastExchangeExec): Option[SparkPlan] = {
    // Same emit-time support check as shuffle. The broadcast serde's getSupportLevel may
    // reject based on child types / structure that weren't decidable at Phase 1.
    if (!CometBroadcastExchangeExec.getSupportLevel(b).isInstanceOf[Compatible]) {
      logDebug(
        s"Phase3: broadcast serde rejected at emit time id=${b.id} " +
          s"support=${CometBroadcastExchangeExec.getSupportLevel(b).getClass.getSimpleName}")
      return None
    }
    assert(allChildrenNative(b), s"emitBroadcast invoked with non-native children id=${b.id}")
    val childOps = b.children.map(nativeOpOf)
    runSerde(b, CometBroadcastExchangeExec, childOps)
  }

  /**
   * Run a serde and, if it returned a placeholder wrapper (`CometSinkPlaceHolder` /
   * `CometScanWrapper`), unwrap to the inner operator and attach the protobuf as a `NATIVE_OP`
   * tag. This is the tag-based replacement for the old placeholder plan-tree wrappers: Phase 3
   * emits the JVM-orchestrated operators directly without a stripping post-pass.
   *
   * The placeholder classes still exist because the legacy rule path (`COMET_USE_PLANNER=false`)
   * produces them. They go away when the legacy rule is deleted.
   */
  private def runSerde[T <: SparkPlan](
      op: T,
      serde: CometOperatorSerde[T],
      childOps: Seq[OperatorOuterClass.Operator]): Option[SparkPlan] = {
    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
    childOps.foreach(builder.addChildren)
    val nativeOpOpt = serde.convert(op, builder, childOps: _*)
    if (nativeOpOpt.isEmpty) {
      logDebug(
        s"Phase3: serde.convert returned None node=${op.getClass.getSimpleName} " +
          s"id=${op.id} serde=${serde.getClass.getSimpleName}")
    }
    nativeOpOpt.map { nativeOp =>
      val raw = serde.createExec(nativeOp, op)
      val exec = raw match {
        case CometSinkPlaceHolder(_, _, inner) =>
          inner.setTagValue(CometTags.NATIVE_OP, nativeOp)
          inner
        case CometScanWrapper(_, inner) =>
          inner.setTagValue(CometTags.NATIVE_OP, nativeOp)
          inner
        case direct => direct
      }
      op.logicalLink.foreach(exec.setLogicalLink)
      exec.setTagValue(CometTags.COMET_CONVERTED, ())
      exec
    }
  }

  private def logEmit(kind: String, in: SparkPlan, out: SparkPlan): Unit = {
    if (log.isTraceEnabled && !(in eq out)) {
      logTrace(
        s"Phase3: emit kind=$kind inClass=${in.getClass.getSimpleName} " +
          s"outClass=${out.getClass.getSimpleName} inId=${in.id} outId=${out.id}")
    }
  }
}
