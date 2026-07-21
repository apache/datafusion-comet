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
import org.apache.spark.sql.CometTestBase

import org.apache.comet.iceberg.CometTestKMS

/**
 * Encryption-specific Iceberg tests. Lives in the spark-4.1 source set because Iceberg table
 * encryption is a V3 feature and the encryption APIs (KeyManagementClient, StandardEncryption
 * manager) are only public from Iceberg 1.11, which Comet pairs with Spark 4.1.
 */
class CometIcebergEncryptionSuite extends CometTestBase with CometIcebergTestBase {

  // Confirms the assumption that native encrypted reads will rely on: an Iceberg data file's
  // key_metadata carries a PLAINTEXT data encryption key, not a KMS-wrapped one. Iceberg mints a
  // fresh DEK per file in StandardEncryptionManager.encrypt() and records it plaintext in
  // key_metadata (the manifest that holds it is itself encrypted). The KMS only ever wraps the KEK
  // / manifest-list keys, never the per-file DEK. So Comet can forward these bytes straight to
  // iceberg-rust with no native KMS.
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
      val blob = toArray(keyMetadata.buffer())
      assert(blob(0) == 1, s"unexpected key_metadata version byte: ${blob(0)}")

      val kmClass = Class.forName("org.apache.iceberg.encryption.StandardKeyMetadata")
      val parse = kmClass.getDeclaredMethod("parse", classOf[ByteBuffer])
      parse.setAccessible(true)
      val parsed =
        parse.invoke(null, ByteBuffer.wrap(blob)).asInstanceOf[NativeEncryptionKeyMetadata]
      val roundTripped = toArray(parsed.encryptionKey())

      assert(
        java.util.Arrays.equals(toArray(dek), roundTripped),
        "DEK did not survive the StandardKeyMetadata encode/parse round-trip")

      println(
        s"key_metadata blob ${blob.length} bytes, plaintext DEK ${roundTripped.length} bytes; " +
          "no KMS needed to recover the data-file key")
      // Emit the raw bytes so the Rust side (iceberg-rust StandardKeyMetadata::decode) can be
      // tested against a real Java-produced blob. Paste these into the Rust cross-compat test.
      // println (not logInfo) so the values surface in the IntelliJ test console.
      println(s"JAVA key_metadata blob hex: ${hex(blob)}")
      println(s"JAVA plaintext DEK hex:     ${hex(roundTripped)}")
    }
  }

  private def toArray(buffer: ByteBuffer): Array[Byte] = {
    val dup = buffer.duplicate()
    val bytes = new Array[Byte](dup.remaining())
    dup.get(bytes)
    bytes
  }

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"$b%02x").mkString
}
