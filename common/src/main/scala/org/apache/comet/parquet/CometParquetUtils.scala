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

package org.apache.comet.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.KeyToolkit
import org.apache.spark.sql.internal.SQLConf

object CometParquetUtils {
  private val PARQUET_FIELD_ID_WRITE_ENABLED = "spark.sql.parquet.fieldId.write.enabled"
  private val PARQUET_FIELD_ID_READ_ENABLED = "spark.sql.parquet.fieldId.read.enabled"
  private val IGNORE_MISSING_PARQUET_FIELD_ID = "spark.sql.parquet.fieldId.read.ignoreMissing"

  // Map of unsupported encryption configuration key-value pairs
  private val UNSUPPORTED_ENCRYPTION_CONFIGS: Map[String, Set[String]] = Map(
    "parquet.encryption.algorithm" -> Set("AES_GCM_CTR_V1")
    // Add more unsupported configs here as needed
    // "parquet.encryption.some.config" -> Set("unsupported_value1", "unsupported_value2")
  )

  // Map of encryption configurations that can only have specific allowed values
  private val SUPPORTED_ENCRYPTION_CONFIGS_WHITELIST: Map[String, Set[String]] = Map(
    "parquet.encryption.data.key.length.bits" -> Set("128"),
    "parquet.encryption.kek.length.bits" -> Set("128")
    // Add more whitelisted configs here as needed
    // "parquet.encryption.some.config" -> Set("allowed_value1", "allowed_value2")
  )

  def writeFieldId(conf: SQLConf): Boolean =
    conf.getConfString(PARQUET_FIELD_ID_WRITE_ENABLED, "false").toBoolean

  def writeFieldId(conf: Configuration): Boolean =
    conf.getBoolean(PARQUET_FIELD_ID_WRITE_ENABLED, false)

  def readFieldId(conf: SQLConf): Boolean =
    conf.getConfString(PARQUET_FIELD_ID_READ_ENABLED, "false").toBoolean

  def ignoreMissingIds(conf: SQLConf): Boolean =
    conf.getConfString(IGNORE_MISSING_PARQUET_FIELD_ID, "false").toBoolean

  /**
   * Checks if the given Hadoop configuration contains any unsupported encryption settings.
   *
   * @param hadoopConf
   *   The Hadoop configuration to check
   * @return
   *   true if all encryption configurations are supported, false if any unsupported config is
   *   found
   */
  def isEncryptionConfigSupported(hadoopConf: Configuration): Boolean = {
    // Check blacklist: configurations that should never have certain values
    val blacklistCheck = UNSUPPORTED_ENCRYPTION_CONFIGS.forall {
      case (configKey, unsupportedValues) =>
        val configValue = Option(hadoopConf.get(configKey))
        configValue match {
          case Some(value) => !unsupportedValues.contains(value)
          case None => true // Config not set, so it's supported
        }
    }

    // Check whitelist: configurations that can only have specific allowed values
    val whitelistCheck = SUPPORTED_ENCRYPTION_CONFIGS_WHITELIST.forall {
      case (configKey, allowedValues) =>
        val configValue = Option(hadoopConf.get(configKey))
        configValue match {
          case Some(value) => allowedValues.contains(value)
          case None => true // Config not set, so it's supported
        }
    }

    blacklistCheck && whitelistCheck
  }

  def encryptionEnabled(hadoopConf: Configuration): Boolean = {
    // TODO: Are there any other properties to check?
    val encryptionKeys = Seq(
      DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
      KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME)

    encryptionKeys.exists(key => Option(hadoopConf.get(key)).exists(_.nonEmpty))
  }
}
