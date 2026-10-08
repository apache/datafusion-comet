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

package org.apache.spark.sql.comet

import java.net.URI
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

import org.apache.comet.objectstore.NativeConfig

object CometScanUtils {

  /**
   * Filters unused DynamicPruningExpression expressions - one which has been replaced with
   * DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
   */
  def filterUnusedDynamicPruningExpressions(predicates: Seq[Expression]): Seq[Expression] = {
    // Strip DPP expressions for canonicalization. Matches Spark's
    // FileSourceScanExec.filterUnusedDynamicPruningExpressions (TrueLiteral).
    // Also strips unconverted SAB wrappers because AQE stageCache canonicalizes
    // before our queryStageOptimizerRule converts them, so they would prevent
    // exchange reuse between otherwise-identical scans.
    predicates.filterNot {
      case DynamicPruningExpression(Literal.TrueLiteral) => true
      case DynamicPruningExpression(
            InSubqueryExec(_, _: CometSubqueryAdaptiveBroadcastExec, _, _, _, _)) =>
        true
      case DynamicPruningExpression(
            InSubqueryExec(_, _: SubqueryAdaptiveBroadcastExec, _, _, _, _)) =>
        true
      case _ => false
    }
  }

  /**
   * Bin packs `files` with `pack` separately for each object store `storeKey` names, so no
   * partition holds files from two stores, and numbers the partitions from 0. Files that share
   * one store are packed exactly as `pack` packs them.
   */
  def packFilesPerStore[K](files: Seq[PartitionedFile], storeKey: PartitionedFile => K)(
      pack: Seq[PartitionedFile] => Seq[FilePartition]): Seq[FilePartition] = {
    val groups = groupByStore(files, storeKey)
    if (groups.size <= 1) {
      pack(files)
    } else {
      groups
        .flatMap(pack)
        .zipWithIndex
        .map { case (partition, index) => partition.copy(index = index) }
    }
  }

  /**
   * Splits each partition into one partition per object store `storeKey` names, keeping the file
   * order, and numbers the partitions from 0. Returns `partitions` itself when none mixes stores.
   */
  def splitPartitionsByStore[K](
      partitions: Seq[FilePartition],
      storeKey: PartitionedFile => K): Seq[FilePartition] = {
    val split = partitions.flatMap { partition =>
      val groups = groupByStore(partition.files.toSeq, storeKey)
      if (groups.length <= 1) {
        Seq(partition)
      } else {
        groups.map(files => FilePartition(partition.index, files.toArray))
      }
    }
    if (split.length == partitions.length) {
      partitions
    } else {
      split.zipWithIndex.map { case (partition, index) => partition.copy(index = index) }
    }
  }

  /** `files` grouped by store, in first-seen store order, keeping file order. */
  private def groupByStore[K](
      files: Seq[PartitionedFile],
      storeKey: PartitionedFile => K): Seq[Seq[PartitionedFile]] = {
    val groups = mutable.LinkedHashMap.empty[K, mutable.ArrayBuffer[PartitionedFile]]
    files.foreach { file =>
      groups.getOrElseUpdate(storeKey(file), mutable.ArrayBuffer.empty[PartitionedFile]) += file
    }
    groups.valuesIterator.map(_.toVector).toVector
  }

  /**
   * Why a native scan of `uris` must fall back because of where they live, or None. The scan
   * forwards the settings of every scheme it reads, and translates alias settings to the
   * `fs.s3a.*` keys of the alias bucket. So it falls back when one store is read through an
   * S3-compliant alias and another scheme, or when an alias path has no bucket and other S3 paths
   * would get its settings. A bucketed scan reads each table bucket as one partition, so its
   * paths must share one store. `scanName` starts the reason. `uris` is walked once, so a lazy
   * view of a scan's files works.
   */
  def multiStoreFallbackReason(
      scanName: String,
      uris: Iterable[URI],
      s3CompliantSchemes: Set[String],
      libhdfsSchemes: Set[String],
      isBucketedScan: Boolean): Option[String] = {
    val schemesByStore =
      uris.foldLeft(Map.empty[NativeConfig.ObjectStoreKey, Set[String]]) { (seen, uri) =>
        val store = NativeConfig.objectStoreKey(uri, s3CompliantSchemes, libhdfsSchemes)
        val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("file")
        seen.updated(store, seen.getOrElse(store, Set.empty[String]) + scheme)
      }
    val nativeStores = schemesByStore.toSeq.filterNot(_._1.isLibhdfs).sortBy(_._1.key)
    val sharedAliasStore = nativeStores.find { case (_, schemes) =>
      schemes.size > 1 && schemes.exists(s3CompliantSchemes.contains)
    }
    // An alias path with no bucket translates its settings to the global `fs.s3a.*` keys.
    val s3Paths = nativeStores.flatMap { case (store, schemes) =>
      schemes.toSeq.filter(isS3FamilyScheme(_, s3CompliantSchemes)).map(store -> _)
    }
    val hasBucketlessAlias = s3Paths.exists { case (store, scheme) =>
      store.key == "s3://" && s3CompliantSchemes.contains(scheme)
    }
    if (sharedAliasStore.nonEmpty) {
      val (store, schemes) = sharedAliasStore.get
      Some(
        s"$scanName reads the object store $store through schemes " +
          s"${schemes.toSeq.sorted.mkString(", ")}, but forwards one set of settings per store")
    } else if (hasBucketlessAlias && s3Paths.size > 1) {
      Some(
        s"$scanName reads an S3-compliant alias path with no bucket next to other S3 paths, " +
          "but would apply its alias settings to every bucket")
    } else if (isBucketedScan && schemesByStore.size > 1) {
      val keys = schemesByStore.keys.toSeq.map(_.toString).sorted.mkString(", ")
      Some(
        s"$scanName of a bucketed table reads paths in object stores $keys, but reads each " +
          "table bucket through one store")
    } else {
      None
    }
  }

  private def isS3FamilyScheme(scheme: String, s3CompliantSchemes: Set[String]): Boolean =
    scheme == "s3" || scheme == "s3a" || scheme == "s3n" || s3CompliantSchemes.contains(scheme)
}
