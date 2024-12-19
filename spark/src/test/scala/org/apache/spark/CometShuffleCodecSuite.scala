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

package org.apache.spark

import java.io.FileInputStream

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.{ArrowReaderIterator, IpcInputStreamIterator}

import org.apache.comet.CometConf

/**
 * Manual tests for testing compatibility of shuffle files generated from native tests.
 */
class CometShuffleCodecSuite extends CometTestBase {

  ignore("decode shuffle batch with zstd compression") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_CODEC.key -> "zstd") {
      // test file is created by shuffle_writer.rs test `write_ipc_zstd`
      val is = new FileInputStream("/tmp/shuffle.zstd")
      val ins = IpcInputStreamIterator(is, decompressingNeeded = true, TaskContext.get())
      assert(ins.hasNext)
      val channel = ins.next()
      val it = new ArrowReaderIterator(channel, "test")
      assert(it.hasNext)
      val batch = it.next()
      assert(8192 == batch.numRows())
    }
  }

  test("decode shuffle batch with lz4 compression") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_CODEC.key -> "lz4") {
      // test file is created by shuffle_writer.rs test `write_ipc_lz4`
      val is = new FileInputStream("/tmp/shuffle.lz4_frame")
      val ins = IpcInputStreamIterator(is, decompressingNeeded = true, TaskContext.get())
      assert(ins.hasNext)
      val channel = ins.next()
      val it = new ArrowReaderIterator(channel, "test")
      assert(it.hasNext)
      val batch = it.next()
      assert(8192 == batch.numRows())
    }
  }

}
