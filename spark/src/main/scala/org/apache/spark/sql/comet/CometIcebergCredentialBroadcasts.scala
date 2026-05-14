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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometRuntimeException}
import org.apache.comet.iceberg.{CometCredentialProvider, IcebergReflection}

/**
 * Driver-side broadcast cache for [[CometCredentialProvider]] instances.
 */
private[spark] object CometIcebergCredentialBroadcasts extends Logging {

  private type CatalogName = String

  private val cache =
    new ConcurrentHashMap[CatalogName, Broadcast[AnyRef]]()

  def getOrCreate(
      sc: SparkContext,
      className: String,
      catalogName: String,
      catalogProperties: Map[String, String]): Broadcast[AnyRef] = {

    val existing = cache.get(catalogName)
    if (existing != null) {
      logDebug(
        s"Reusing broadcast credential provider id=${existing.id} " +
          s"className=$className catalogName=$catalogName")
      return existing
    }

    cache.computeIfAbsent(
      catalogName,
      _ => {
        val provider = instantiateAndInit(className, catalogProperties)
        val broadcast = sc.broadcast[AnyRef](provider)
        logInfo(
          s"Broadcast credential provider created id=${broadcast.id} " +
            s"className=$className catalogName=$catalogName")
        broadcast
      })
  }

  private def instantiateAndInit(
      className: String,
      catalogProperties: Map[String, String]): CometCredentialProvider = {
    val provider = IcebergReflection
      .loadClass(className)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[CometCredentialProvider]
    provider.initialize(catalogProperties.asJava)
    provider
  }

  def buildBroadcast(
      sc: SparkContext,
      scan: CometIcebergNativeScanExec): Option[Broadcast[AnyRef]] = {
    val broadcast = for {
      className <- CometConf.COMET_ICEBERG_CREDENTIAL_PROVIDER_CLASS.get(SQLConf.get)
      catalogName <- scan.nativeIcebergScanMetadata.catalogName
    } yield getOrCreate(
      sc,
      className,
      catalogName,
      scan.nativeIcebergScanMetadata.allFileIOProperties)
    broadcast.foreach { b =>
      logInfo(
        s"Plan will use broadcast credential provider id=${b.id} " +
          s"catalogName=${scan.nativeIcebergScanMetadata.catalogName.getOrElse("")}")
    }
    broadcast
  }

  /**
   * Walks the plan, validates a single Iceberg catalog, and builds the broadcast for the first
   * matched scan.
   *
   * @throws CometRuntimeException
   *   if the plan spans more than one Iceberg catalog (only one `CometCredentialProvider` is
   *   broadcast per plan).
   */
  def buildBroadcastForPlan(
      sc: SparkContext,
      plan: CometNativeExec): Option[Broadcast[AnyRef]] = {
    val icebergScans = scala.collection.mutable.ArrayBuffer.empty[CometIcebergNativeScanExec]
    plan.foreachUntilCometInput(plan) {
      case scan: CometIcebergNativeScanExec => icebergScans += scan
      case _ => // no-op
    }
    requireSingleIcebergCatalog(
      icebergScans.flatMap(_.nativeIcebergScanMetadata.catalogName).toSeq)
    icebergScans.headOption.flatMap(buildBroadcast(sc, _))
  }

  private[comet] def requireSingleIcebergCatalog(catalogNames: Seq[String]): Unit = {
    val distinct = catalogNames.distinct
    if (distinct.size > 1) {
      throw new CometRuntimeException(
        s"Comet plan spans multiple Iceberg catalogs (${distinct.mkString(", ")}), " +
          s"but only one CometCredentialProvider is broadcast per plan. Multi-catalog " +
          s"plans need a per-relation broadcast map.")
    }
  }

  private[comet] def clear(): Unit = {
    cache.clear()
  }

  private[comet] def cacheSize(): Int = cache.size
}
