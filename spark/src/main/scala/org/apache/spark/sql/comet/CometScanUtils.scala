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
    val keyed = files.map(file => (storeKey(file), file))
    val stores = keyed.map(_._1).distinct
    if (stores.size <= 1) {
      pack(files)
    } else {
      stores
        .flatMap(store => pack(keyed.collect { case (key, file) if key == store => file }))
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
      val keyed = partition.files.toSeq.map(file => (storeKey(file), file))
      val stores = keyed.map(_._1).distinct
      if (stores.length <= 1) {
        Seq(partition)
      } else {
        stores.map { store =>
          val files = keyed.collect { case (key, file) if key == store => file }
          FilePartition(partition.index, files.toArray)
        }
      }
    }
    if (split.length == partitions.length) {
      partitions
    } else {
      split.zipWithIndex.map { case (partition, index) => partition.copy(index = index) }
    }
  }

  /**
   * Why a native scan of `uris` must fall back because of where they live, or None. The scan
   * forwards the object store settings of one scheme, so paths of different scheme families
   * (s3/s3a/s3n, libhdfs, or any other scheme on its own) cannot share it, even in one bucket. A
   * bucketed scan reads each table bucket as one partition, so its paths must share one store.
   * `scanName` starts the reason. `uris` is walked once, so a lazy view of a scan's files works.
   */
  def multiStoreFallbackReason(
      scanName: String,
      uris: Iterable[URI],
      s3CompliantSchemes: Set[String],
      libhdfsSchemes: Set[String],
      isBucketedScan: Boolean): Option[String] = {
    val (schemes, stores) =
      uris.foldLeft((Set.empty[Option[String]], Set.empty[NativeConfig.ObjectStoreKey])) {
        case ((schemes, stores), uri) =>
          val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT))
          val nextStores =
            if (isBucketedScan) {
              stores + NativeConfig.objectStoreKey(uri, s3CompliantSchemes, libhdfsSchemes)
            } else {
              stores
            }
          (schemes + scheme, nextStores)
      }
    val families = schemes.map {
      case Some(scheme) if libhdfsSchemes.contains(scheme) => "libhdfs"
      case Some("s3" | "s3a" | "s3n") => "s3"
      case scheme => scheme.getOrElse("file")
    }
    if (families.size > 1) {
      val names = schemes.toSeq.map(_.getOrElse("file")).distinct.sorted
      Some(
        s"$scanName reads paths with schemes ${names.mkString(", ")}, whose object store " +
          "settings differ, but forwards the settings of one scheme")
    } else if (stores.size > 1) {
      val keys = stores.toSeq.map(_.toString).sorted.mkString(", ")
      Some(
        s"$scanName of a bucketed table reads paths in object stores $keys, but reads each " +
          "table bucket through one store")
    } else {
      None
    }
  }
}
