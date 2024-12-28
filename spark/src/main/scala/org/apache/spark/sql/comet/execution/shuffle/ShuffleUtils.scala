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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{IO_COMPRESSION_CODEC, SHUFFLE_COMPRESS}
import org.apache.spark.io.CompressionCodec

import org.apache.comet.CometConf

private[spark] object ShuffleUtils extends Logging {
  // optional compression codec to use when compressing shuffle files
  lazy val compressionCodecForShuffling: Option[CompressionCodec] = {
    val sparkConf = SparkEnv.get.conf
    val shuffleCompressionEnabled = sparkConf.getBoolean(SHUFFLE_COMPRESS.key, true)
    val sparkShuffleCodec = sparkConf.get(IO_COMPRESSION_CODEC.key, "lz4")
    val cometShuffleCodec = CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC.get()
    if (shuffleCompressionEnabled) {
      if (sparkShuffleCodec != cometShuffleCodec) {
        logWarning(
          s"Overriding config $IO_COMPRESSION_CODEC=$sparkShuffleCodec in shuffling, " +
            s"force using $cometShuffleCodec")
      }
      cometShuffleCodec match {
        case "zstd" =>
          Some(CompressionCodec.createCodec(sparkConf, "zstd"))
        case other =>
          throw new UnsupportedOperationException(
            s"Unsupported shuffle compression codec: $other")
      }
    } else {
      None
    }
  }
}
