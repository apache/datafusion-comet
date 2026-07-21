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

import java.nio.ByteBuffer
import java.util.Collections

import org.apache.iceberg.Files
import org.apache.iceberg.encryption.{EncryptedKey, NativeEncryptionKeyMetadata, StandardEncryptionManager}
import org.apache.iceberg.util.ByteBuffers
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.iceberg.CometTestKMS

/**
 * Encryption-specific Iceberg tests. Lives in the spark-4.1 source set because Iceberg table
 * encryption is a V3 feature and the encryption APIs (KeyManagementClient, StandardEncryption
 * manager) are only public from Iceberg 1.11, which Comet pairs with Spark 4.1.
 */
class CometIcebergEncryptionSuite
    extends CometTestBase
    with CometIcebergTestBase
    with AdaptiveSparkPlanHelper {

  // A Hive catalog is required for an actual encrypted table: it is the only Iceberg catalog that
  // wires the KMS-backed EncryptionManager in 1.11 (Hadoop/REST write plaintext). Point Iceberg's
  // type=hive catalog at an in-memory Derby metastore so no external HMS is needed. Set at session
  // creation because Iceberg builds its HiveConf from the Hadoop configuration, which reflects
  // spark.hadoop.* only at context startup, not from runtime withSQLConf.
  //
  // lazy so the temp dir is created at session init, not at construction: CI runs tests with
  // -DwildcardSuites, which puts ScalaTest in discovery mode and instantiates every suite before
  // any test executes. createTempDirectory does not create its parent (java.io.tmpdir, pinned to
  // target/tmp by the pom), which does not exist yet on a fresh checkout, so an eager val aborts
  // discovery with NoSuchFileException. createDirectories makes the parent up front.
  private lazy val hiveWarehouse = {
    val base = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"))
    java.nio.file.Files.createDirectories(base)
    java.nio.file.Files.createTempDirectory(base, "comet-hive-warehouse").toFile
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:derby:memory:comet_iceberg_encryption;create=true")
      .set("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
      .set("spark.hadoop.hive.metastore.schema.verification", "false")
      // Hive's default metastore connection pool is BoneCP, which Spark's Hive fork does not ship
      // (NoClassDefFoundError: com/jolbox/bonecp/BoneCPConfig). This config drives both the
      // DataNucleus ObjectStore and the TxnHandler pool, so DBCP steers the embedded metastore off
      // BoneCP. Mirrors Iceberg's TestHiveMetastore.initConf.
      .set("spark.hadoop.datanucleus.connectionPoolingType", "DBCP")
      .set("spark.hadoop.hive.in.test", "true")
      // Skip Iceberg's HMS table lock. The embedded metastore's ACID TxnHandler rejects Iceberg's
      // lock request ("Bug: operationType=UNSET"); disabling it uses the lock-free commit path
      // (atomic alter-table with an expected-parameter check), which is all this single-writer test
      // needs.
      .set("spark.hadoop.iceberg.engine.hive.lock-enabled", "false")
      .set("spark.hadoop.hive.metastore.warehouse.dir", hiveWarehouse.toURI.toString)
      .set("spark.sql.catalog.hive_cat", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hive_cat.type", "hive")
      .set("spark.sql.catalog.hive_cat.warehouse", hiveWarehouse.toURI.toString)
      .set("spark.sql.catalog.hive_cat.encryption.kms-impl", classOf[CometTestKMS].getName)
  }

  // End-to-end: an encrypted V3 table is read through Comet's native Iceberg scan and matches
  // Spark. This is the guard that the encrypted path stays native; Iceberg's own TestTableEncryption
  // (run under Comet in CI) only asserts correctness, not that the native scan was used.
  test("encrypted V3 table is read natively and matches Spark") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(icebergVersionAtLeast(1, 11), "Iceberg table encryption requires Iceberg 1.11+")

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

      val table = "hive_cat.db.encrypted"
      // The Hive catalog does not auto-create namespaces the way the Hadoop catalog does.
      spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_cat.db")
      spark.sql(s"DROP TABLE IF EXISTS $table")
      spark.sql(s"""
        CREATE TABLE $table (id INT, name STRING, value DOUBLE) USING iceberg
        TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '${CometTestKMS.MasterKeyId}')
      """)
      spark.sql(s"""
        INSERT INTO $table VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)
      """)

      // Guard against a silently-plaintext table (e.g. catalog not wiring encryption): a genuinely
      // encrypted table records a plaintext DEK blob per data file in key_metadata.
      val keyMetadatas = spark
        .sql(s"SELECT key_metadata FROM $table.files WHERE content = 0")
        .collect()
        .flatMap(r => Option(r.getAs[Array[Byte]]("key_metadata")))
      assert(
        keyMetadatas.nonEmpty,
        "expected encryption to populate key_metadata; the Hive catalog may not have wired the KMS")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $table ORDER BY id")
      assert(
        collect(cometPlan) { case s: CometIcebergNativeScanExec => s }.nonEmpty,
        "expected the encrypted read to run natively but found no CometIcebergNativeScanExec:\n" +
          cometPlan)

      spark.sql(s"DROP TABLE $table")
    }
  }

  // Confirms the assumption that native encrypted reads rely on: an Iceberg data file's
  // key_metadata carries a PLAINTEXT data encryption key, not a KMS-wrapped one. Iceberg mints a
  // fresh DEK per file in StandardEncryptionManager.encrypt() and records it plaintext in
  // key_metadata (the manifest that holds it is itself encrypted). The KMS only wraps the KEK /
  // manifest-list keys, never the per-file DEK. Runs without a catalog.
  test("data file key_metadata carries a plaintext DEK that round-trips") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(icebergVersionAtLeast(1, 11), "Iceberg table encryption requires Iceberg 1.11+")

    val dataKeyLength = 16
    val mgr = new StandardEncryptionManager(
      Collections.emptyList[EncryptedKey](),
      CometTestKMS.MasterKeyId,
      dataKeyLength,
      new CometTestKMS)

    withTempDir { dir =>
      // encrypt() lazily generates the per-file DEK + AAD prefix; no bytes are written.
      val keyMetadata =
        mgr.encrypt(Files.localOutput(new java.io.File(dir, "data.parquet"))).keyMetadata()

      val dek = keyMetadata.encryptionKey()
      assert(
        dek.remaining() == dataKeyLength,
        s"expected a plaintext $dataKeyLength-byte DEK but got ${dek.remaining()} bytes")
      val aad = keyMetadata.aadPrefix()
      assert(aad != null && aad.remaining() > 0, "expected a non-empty AAD prefix")

      // buffer() is the serialized StandardKeyMetadata blob stored in the manifest's key_metadata
      // field, and the exact bytes Comet would forward to iceberg-rust. It must start with the V1
      // marker and round-trip back to the same DEK. StandardKeyMetadata.parse is package-private,
      // so reach it reflectively, but read the result through the public NativeEncryptionKeyMetadata
      // interface (the concrete class is not accessible). iceberg-rust decodes this same format.
      val blob = ByteBuffers.toByteArray(keyMetadata.buffer())
      assert(blob(0) == 1, s"unexpected key_metadata version byte: ${blob(0)}")

      val kmClass = Class.forName("org.apache.iceberg.encryption.StandardKeyMetadata")
      val parse = kmClass.getDeclaredMethod("parse", classOf[ByteBuffer])
      parse.setAccessible(true)
      val parsed =
        parse.invoke(null, ByteBuffer.wrap(blob)).asInstanceOf[NativeEncryptionKeyMetadata]
      val roundTripped = ByteBuffers.toByteArray(parsed.encryptionKey())

      assert(
        java.util.Arrays.equals(ByteBuffers.toByteArray(dek), roundTripped),
        "DEK did not survive the StandardKeyMetadata encode/parse round-trip")
    }
  }

  // The native Parquet reader (arrow-rs 58.3) decrypts only 128-bit AES-GCM keys, so a table with a
  // larger data key must fall back to Spark and still return correct results. Flip this to a native
  // assertion when arrow-rs 58.4 (256-bit support, arrow-rs#10349) is picked up.
  test("encrypted V3 table with a 256-bit data key falls back to Spark") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(icebergVersionAtLeast(1, 11), "Iceberg table encryption requires Iceberg 1.11+")

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

      val table = "hive_cat.db.encrypted_256"
      spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_cat.db")
      spark.sql(s"DROP TABLE IF EXISTS $table")
      spark.sql(s"""
        CREATE TABLE $table (id INT, name STRING) USING iceberg
        TBLPROPERTIES ('format-version' = '3', 'encryption.key-id' = '${CometTestKMS.MasterKeyId}',
          'encryption.data-key-length' = '32')
      """)
      spark.sql(s"INSERT INTO $table VALUES (1, 'Alice'), (2, 'Bob')")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $table ORDER BY id")
      assert(
        collect(cometPlan) { case s: CometIcebergNativeScanExec => s }.isEmpty,
        "expected a 256-bit encrypted table to fall back to Spark but it ran natively:\n" +
          cometPlan)

      spark.sql(s"DROP TABLE $table")
    }
  }
}
