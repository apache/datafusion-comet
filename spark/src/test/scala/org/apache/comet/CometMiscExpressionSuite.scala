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

import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

class CometMiscExpressionSuite extends CometTestBase {

  test("aes_decrypt") {
    withTempView("aes_tbl") {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val aesDf = if (isSpark35Plus) {
          spark
            .range(1)
            .selectExpr(
              "aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8')) as encrypted_default",
              "aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'), 'GCM', 'DEFAULT', unhex('00112233445566778899AABB'), 'Comet AAD') as encrypted_with_aad",
              "encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8') as `key`",
              "'GCM' as mode",
              "'DEFAULT' as padding",
              "unhex('00112233445566778899AABB') as iv",
              "'Comet AAD' as aad")
        } else {
          spark
            .range(1)
            .selectExpr(
              "aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8')) as encrypted_default",
              "aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'), 'GCM', 'DEFAULT') as encrypted_with_aad",
              "encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8') as `key`",
              "'GCM' as mode",
              "'DEFAULT' as padding",
              "cast(null as binary) as iv",
              "cast(null as string) as aad")
        }
        aesDf.createOrReplaceTempView("aes_tbl")
      }

      if (isSpark35Plus) {
        checkSparkAnswerAndOperator(
          "SELECT CAST(aes_decrypt(encrypted_default, `key`) AS STRING) FROM aes_tbl")
        checkSparkAnswerAndOperator(
          "SELECT CAST(aes_decrypt(encrypted_with_aad, `key`, mode, padding, aad) AS STRING) FROM aes_tbl")
      } else {
        checkSparkAnswerAndOperator(
          "SELECT CAST(aes_decrypt(encrypted_default, `key`) AS STRING) FROM aes_tbl")
        checkSparkAnswerAndOperator(
          "SELECT CAST(aes_decrypt(encrypted_with_aad, `key`, mode, padding) AS STRING) FROM aes_tbl")
      }
    }
  }

  test("aes_decrypt mode and key-size combinations") {
    withTempView("aes_modes_tbl") {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .sql("""
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop', 'UTF-8'), 'GCM', 'DEFAULT') AS encrypted,
            |  encode('abcdefghijklmnop', 'UTF-8') AS `key`,
            |  'GCM' AS mode,
            |  'DEFAULT' AS padding,
            |  'gcm_128' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678', 'UTF-8'), 'GCM', 'DEFAULT') AS encrypted,
            |  encode('abcdefghijklmnop12345678', 'UTF-8') AS `key`,
            |  'GCM' AS mode,
            |  'DEFAULT' AS padding,
            |  'gcm_192' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'), 'GCM', 'DEFAULT') AS encrypted,
            |  encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8') AS `key`,
            |  'GCM' AS mode,
            |  'DEFAULT' AS padding,
            |  'gcm_256' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop', 'UTF-8'), 'CBC', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop', 'UTF-8') AS `key`,
            |  'CBC' AS mode,
            |  'PKCS' AS padding,
            |  'cbc_128' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678', 'UTF-8'), 'CBC', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop12345678', 'UTF-8') AS `key`,
            |  'CBC' AS mode,
            |  'PKCS' AS padding,
            |  'cbc_192' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'), 'CBC', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8') AS `key`,
            |  'CBC' AS mode,
            |  'PKCS' AS padding,
            |  'cbc_256' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop', 'UTF-8'), 'ECB', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop', 'UTF-8') AS `key`,
            |  'ECB' AS mode,
            |  'PKCS' AS padding,
            |  'ecb_128' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678', 'UTF-8'), 'ECB', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop12345678', 'UTF-8') AS `key`,
            |  'ECB' AS mode,
            |  'PKCS' AS padding,
            |  'ecb_192' AS label
            |UNION ALL
            |SELECT
            |  aes_encrypt(encode('Spark SQL', 'UTF-8'), encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'), 'ECB', 'PKCS') AS encrypted,
            |  encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8') AS `key`,
            |  'ECB' AS mode,
            |  'PKCS' AS padding,
            |  'ecb_256' AS label
            |UNION ALL
            |SELECT
            |  cast(null AS binary) AS encrypted,
            |  encode('abcdefghijklmnop', 'UTF-8') AS `key`,
            |  'GCM' AS mode,
            |  'DEFAULT' AS padding,
            |  'null_input' AS label
            |""".stripMargin)
          .createOrReplaceTempView("aes_modes_tbl")
      }

      checkSparkAnswerAndOperator("""
          |SELECT
          |  label,
          |  CAST(aes_decrypt(encrypted, `key`, mode, padding) AS STRING) AS decrypted
          |FROM aes_modes_tbl
          |ORDER BY label
          |""".stripMargin)
    }
  }

}
