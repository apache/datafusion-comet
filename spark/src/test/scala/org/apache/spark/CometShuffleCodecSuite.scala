package org.apache.spark

import org.apache.comet.CometConf

import java.io.FileInputStream
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.{ArrowReaderIterator, IpcInputStreamIterator}
import org.apache.spark.sql.internal.SQLConf

import java.nio.channels.Channels

class CometShuffleCodecSuite extends CometTestBase {

  test("decode shuffle batch with zstd compression") {
    withSQLConf(CometConf.COMET_EXEC_SHUFFLE_CODEC.key -> "zstd") {
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
    // TODO
  }

}
