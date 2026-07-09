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

package org.apache.iceberg.data

import scala.jdk.CollectionConverters._

import org.apache.iceberg.{DeleteFile, Schema, StructLike, Table}
import org.apache.iceberg.encryption.{EncryptedFiles, EncryptionKeyMetadata}
import org.apache.iceberg.io.OutputFile

/**
 * Test-only helper for writing an equality-delete file, mirroring the equality-delete branch of
 * Iceberg's own `FileHelpers.writeDeleteFile`.
 *
 * It lives in `org.apache.iceberg.data` because `GenericFileWriterFactory.builderFor` and its
 * builder are package-private. `FileHelpers` itself is not on Comet's test classpath (it ships
 * only in the iceberg-data test jar, which Apache does not publish), and Spark SQL cannot emit
 * equality deletes (MOR writes only position deletes), so tests that need equality deletes must
 * write the file directly. The writer primitives used here are API-stable across the Iceberg
 * versions Comet builds against (1.5-1.11), so this compiles under every Spark profile.
 */
object CometEqualityDeletes {

  def write(
      table: Table,
      out: OutputFile,
      partition: StructLike,
      deletes: java.util.List[Record],
      deleteRowSchema: Schema): DeleteFile = {
    val equalityFieldIds = deleteRowSchema.columns().asScala.map(_.fieldId()).toArray
    val factory = GenericFileWriterFactory
      .builderFor(table)
      .equalityDeleteRowSchema(deleteRowSchema)
      .equalityFieldIds(equalityFieldIds)
      .build()
    val writer = factory.newEqualityDeleteWriter(
      EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY),
      table.spec(),
      partition)
    try {
      writer.write(deletes)
    } finally {
      writer.close()
    }
    writer.toDeleteFile()
  }
}
