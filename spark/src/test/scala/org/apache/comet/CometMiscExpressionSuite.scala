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
    withTable("aes_tbl") {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sql("""
            |CREATE TABLE aes_tbl(
            |  encrypted_default BINARY,
            |  encrypted_with_aad BINARY,
            |  `key` BINARY,
            |  mode STRING,
            |  padding STRING,
            |  iv BINARY,
            |  aad STRING
            |) USING parquet
            |""".stripMargin)

        if (isSpark35Plus) {
          sql("""
              |INSERT INTO aes_tbl
              |SELECT
              |  aes_encrypt(
              |    encode('Spark SQL', 'UTF-8'),
              |    encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8')),
              |  aes_encrypt(
              |    encode('Spark SQL', 'UTF-8'),
              |    encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'),
              |    'GCM',
              |    'DEFAULT',
              |    unhex('00112233445566778899AABB'),
              |    'Comet AAD'),
              |  encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'),
              |  'GCM',
              |  'DEFAULT',
              |  unhex('00112233445566778899AABB'),
              |  'Comet AAD'
              |""".stripMargin)
        } else {
          sql("""
              |INSERT INTO aes_tbl
              |SELECT
              |  aes_encrypt(
              |    encode('Spark SQL', 'UTF-8'),
              |    encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8')),
              |  aes_encrypt(
              |    encode('Spark SQL', 'UTF-8'),
              |    encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'),
              |    'GCM',
              |    'DEFAULT'),
              |  encode('abcdefghijklmnop12345678ABCDEFGH', 'UTF-8'),
              |  'GCM',
              |  'DEFAULT',
              |  cast(null as binary),
              |  cast(null as string)
              |""".stripMargin)
        }
      }

      if (isSpark35Plus) {
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(aes_decrypt(encrypted_default, `key`) AS STRING) FROM aes_tbl",
          "Static invoke expression: aesDecrypt is not supported")
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(aes_decrypt(encrypted_with_aad, `key`, mode, padding, aad) AS STRING) FROM aes_tbl",
          "Static invoke expression: aesDecrypt is not supported")
      } else {
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(aes_decrypt(encrypted_default, `key`) AS STRING) FROM aes_tbl",
          "Static invoke expression: aesDecrypt is not supported")
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(aes_decrypt(encrypted_with_aad, `key`, mode, padding) AS STRING) FROM aes_tbl",
          "Static invoke expression: aesDecrypt is not supported")
      }
    }
  }

}
