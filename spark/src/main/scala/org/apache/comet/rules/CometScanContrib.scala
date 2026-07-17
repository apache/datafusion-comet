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

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Optional scan-rule extension point for contrib data sources.
 *
 * Implementations are discovered with `ServiceLoader` so core can delegate V2 scan conversion
 * without holding compile-time or reflective references to contrib classes.
 */
trait CometScanContrib {
  def tryTransform(scanExec: BatchScanExec): Option[SparkPlan]
}

object CometScanContrib extends Logging {

  private lazy val contribs: Seq[CometScanContrib] =
    try {
      ServiceLoader.load(classOf[CometScanContrib], getClass.getClassLoader).asScala.toSeq
    } catch {
      case NonFatal(e) =>
        logWarning(
          "Failed to load contrib CometScanContrib services; " +
            "continuing with built-in scan rules only",
          e)
        Seq.empty
    }

  def tryTransform(scanExec: BatchScanExec): Option[SparkPlan] = {
    val iterator = contribs.iterator
    var transformed: Option[SparkPlan] = None
    while (transformed.isEmpty && iterator.hasNext) {
      val contrib = iterator.next()
      try {
        transformed = contrib.tryTransform(scanExec)
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Contrib scan rule ${contrib.getClass.getName} failed; " +
              "continuing with the next scan rule",
            e)
      }
    }
    transformed
  }
}
