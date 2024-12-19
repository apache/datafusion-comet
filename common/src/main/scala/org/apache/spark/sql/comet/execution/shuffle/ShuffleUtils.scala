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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.{InputStream, OutputStream}

import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

private[spark] object ShuffleUtils extends Logging {
  lazy val compressionCodecForShuffling: Option[CompressionCodec] = {
    val sparkConf = SparkEnv.get.conf
    val codecName = CometConf.COMET_EXEC_SHUFFLE_CODEC.get(SQLConf.get)
    codecName match {
      // TODO use separate config to disable compression, using "none" for now while experimenting
      case "none" => None
      case "zstd" => Some(CompressionCodec.createCodec(sparkConf, "zstd"))
      case "lz4_block" => Some(Lz4BlockCodec)
      case "lz4_frame" => Some(Lz4FrameCodec)
      case other =>
        throw new IllegalStateException(s"Unsupported shuffle compression codec: $other")
    }
  }
}

object Lz4BlockCodec extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    throw new UnsupportedOperationException()
  }

  override def compressedInputStream(s: InputStream): InputStream =
    new BlockLZ4CompressorInputStream(s)
}

object Lz4FrameCodec extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    throw new UnsupportedOperationException()
  }

  override def compressedInputStream(s: InputStream): InputStream =
    new FramedLZ4CompressorInputStream(s)
}
