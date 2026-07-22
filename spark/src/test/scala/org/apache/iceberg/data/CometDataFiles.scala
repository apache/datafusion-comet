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

import org.apache.iceberg.{DataFile, StructLike, Table}
import org.apache.iceberg.encryption.{EncryptedFiles, EncryptionKeyMetadata}
import org.apache.iceberg.io.OutputFile

object CometDataFiles {

  def write(
      table: Table,
      out: OutputFile,
      partition: StructLike,
      records: java.util.List[Record]): DataFile = {
    val factory = GenericFileWriterFactory.builderFor(table).build()
    val writer = factory.newDataWriter(
      EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY),
      table.spec(),
      partition)
    try {
      records.asScala.foreach(writer.write)
    } finally {
      writer.close()
    }
    writer.toDataFile()
  }
}
