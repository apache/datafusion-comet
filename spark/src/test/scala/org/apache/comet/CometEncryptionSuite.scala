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

class CometEncryptionSuite extends CometTestBase {

  test("aes_encrypt ECB mode - deterministic") {
    // ECB mode doesn't use IV, so results are deterministic
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES
        ('Spark', '0000111122223333'),
        ('SQL', 'abcdefghijklmnop')
      """)

      val query = """
        SELECT
          data,
          hex(aes_encrypt(cast(data as binary), cast(key as binary), 'ECB', 'PKCS')) as encrypted
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }

  test("aes_encrypt CBC mode with fixed IV - deterministic") {
    // CBC mode with explicit IV is deterministic
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES ('test', '1234567890123456')
      """)

      val query = """
        SELECT hex(aes_encrypt(
          cast(data as binary),
          cast(key as binary),
          'CBC',
          'PKCS',
          cast(unhex('00000000000000000000000000000000') as binary)
        ))
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }

  test("aes_encrypt GCM mode with fixed IV - deterministic") {
    // GCM mode with explicit IV is deterministic
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES ('Spark', '0000111122223333')
      """)

      val query = """
        SELECT hex(aes_encrypt(
          cast(data as binary),
          cast(key as binary),
          'GCM',
          'DEFAULT',
          cast(unhex('000000000000000000000000') as binary)
        ))
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }

  test("aes_encrypt GCM mode with AAD") {
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES ('Spark', 'abcdefghijklmnop12345678ABCDEFGH')
      """)

      val query = """
        SELECT hex(aes_encrypt(
          cast(data as binary),
          cast(key as binary),
          'GCM',
          'DEFAULT',
          cast(unhex('000000000000000000000000') as binary),
          cast('additional authenticated data' as binary)
        ))
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }

  test("aes_encrypt with multiple rows") {
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES
        ('message1', 'key1key1key1key1'),
        ('message2', 'key2key2key2key2'),
        ('message3', 'key3key3key3key3')
      """)

      val query = """
        SELECT
          data,
          hex(aes_encrypt(
            cast(data as binary),
            cast(key as binary),
            'ECB',
            'PKCS'
          )) as encrypted
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }

  test("aes_encrypt wrapped in multiple functions") {
    withTable("t1") {
      sql("""
        CREATE TABLE t1(data STRING, key STRING) USING parquet
      """)
      sql("""
        INSERT INTO t1 VALUES ('test', '1234567890123456')
      """)

      val query = """
        SELECT
          hex(aes_encrypt(cast(data as binary), cast(key as binary), 'ECB', 'PKCS')) as encrypted,
          length(hex(aes_encrypt(cast(data as binary), cast(key as binary), 'ECB', 'PKCS'))) as len
        FROM t1
      """

      checkSparkAnswerAndOperator(query)
    }
  }
}
