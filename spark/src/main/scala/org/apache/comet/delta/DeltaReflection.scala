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

package org.apache.comet.delta

import org.apache.spark.internal.Logging

/**
 * Shared reflection utilities for Delta operations.
 *
 * This object provides common reflection methods used across Comet for interacting with Delta
 * classes without requiring a runtime dependency on Delta
 */
object DeltaReflection extends Logging {

  /**
   * Gets the Delta Protocol from a Spark FileFormat
   */
  def getProtocol(fileFormat: Any): Option[Any] = {
    try {
      val field = fileFormat.getClass.getDeclaredField("protocol")
      field.setAccessible(true)
      Some(field.get(fileFormat))
    } catch {
      case e: Exception =>
        logError(
          s"Delta reflection failure: Failed to get protocol from FileFormat: ${e.getMessage}")
        None
    }
  }

  /**
   * Gets the tasks from a SparkScan.
   *
   * The tasks() method is protected in SparkScan, requiring reflection to access.
   */
  def getMinReaderVersion(protocol: Any): Option[Int] = {
    try {
      val field = protocol.getClass.getDeclaredField("minReaderVersion")
      field.setAccessible(true)
      Some(field.get(protocol).asInstanceOf[Int])
    } catch {
      case e: Exception =>
        logError(
          "Delta reflection failure: Failed to get minReaderVersion from protocol: " +
            s"${e.getMessage}")
        None
    }
  }

  def getReaderFeatures(protocol: Any): Option[Set[String]] = {
    try {
      val field = protocol.getClass.getDeclaredField("readerFeatures")
      field.setAccessible(true)
      Some(field.get(protocol).asInstanceOf[Option[Set[String]]].getOrElse(Set.empty))
    } catch {
      case e: Exception =>
        logError(
          "Delta reflection failure: Failed to get minReaderVersion from protocol: " +
            s"${e.getMessage}")
        None
    }
  }
}

/**
 * Pre-extracted Delta metadata for native scan execution.
 *
 * This class holds all metadata extracted from Delta during the planning/validation phase in
 * CometScanRule. By extracting all metadata once during validation (where reflection failures
 * trigger fallback to Spark), we avoid redundant reflection during serialization (where failures
 * would be fatal runtime errors).
 *
 * @param minReaderVersion
 *   The minimum reader version of the table
 * @param readerFeatures
 *   A list of enabled reader features on the table
 */
case class CometDeltaNativeScanMetadata(minReaderVersion: Int, readerFeatures: Set[String])

object CometDeltaNativeScanMetadata extends Logging {
  import DeltaReflection._

  /**
   * Extracts all Delta metadata needed for native scan execution.
   *
   * This method performs all reflection operations once during planning/validation. If any
   * reflection operation fails, returns None to trigger fallback to Spark.
   *
   * @param fileFormat
   *   The FileFormat instance from the HadoopFsRelation
   * @return
   *   Some(metadata) if all reflection succeeds, None to trigger fallback
   */
  def extract(fileFormat: Any): Option[CometDeltaNativeScanMetadata] = {
    getProtocol(fileFormat).flatMap { protocol =>
      for {
        minReaderVersion <- getMinReaderVersion(protocol)
        readerFeatures <- getReaderFeatures(protocol)
      } yield {
        CometDeltaNativeScanMetadata(minReaderVersion, readerFeatures)
      }
    }
  }
}
