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

package org.apache.comet.iceberg

import java.nio.ByteBuffer
import java.util

import org.apache.iceberg.encryption.Ciphers
import org.apache.iceberg.encryption.KeyManagementClient

/**
 * In-memory KMS for exercising Iceberg table encryption in tests. Iceberg instantiates this by
 * class name via the `encryption.kms-impl` catalog property, so it needs a public no-arg
 * constructor. Wrapping/unwrapping mirrors Iceberg's own MemoryMockKMS (AES-GCM under a fixed
 * master key); it is self-consistent, which is all Iceberg requires. Not for production use.
 */
class CometTestKMS extends KeyManagementClient {

  override def wrapKey(key: ByteBuffer, wrappingKeyId: String): ByteBuffer = {
    val masterKey = CometTestKMS.masterKey(wrappingKeyId)
    val encryptor = new Ciphers.AesGcmEncryptor(masterKey)
    ByteBuffer.wrap(encryptor.encrypt(toArray(key), null))
  }

  override def unwrapKey(wrappedKey: ByteBuffer, wrappingKeyId: String): ByteBuffer = {
    val masterKey = CometTestKMS.masterKey(wrappingKeyId)
    val decryptor = new Ciphers.AesGcmDecryptor(masterKey)
    ByteBuffer.wrap(decryptor.decrypt(toArray(wrappedKey), null))
  }

  override def initialize(properties: util.Map[String, String]): Unit = {}

  private def toArray(buffer: ByteBuffer): Array[Byte] = {
    val dup = buffer.duplicate()
    val bytes = new Array[Byte](dup.remaining())
    dup.get(bytes)
    bytes
  }
}

object CometTestKMS {
  // 16-byte master key; keyed by the id referenced in the table's `encryption.key-id` property.
  val MasterKeyId = "keyA"
  private val MasterKey = "0123456789012345".getBytes("UTF-8")

  private def masterKey(wrappingKeyId: String): Array[Byte] = {
    if (wrappingKeyId != MasterKeyId) {
      throw new RuntimeException(s"Unknown wrapping key id: $wrappingKeyId")
    }
    MasterKey
  }
}
