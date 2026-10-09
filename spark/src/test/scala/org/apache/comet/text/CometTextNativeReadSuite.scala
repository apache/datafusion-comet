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

package org.apache.comet.text

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometTextNativeScanExec
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

class CometTextNativeReadSuite extends CometTestBase {
  private val TEST_TEXT_PATH = "src/test/resources/test-data/text-test-1.txt"

  private def withTextConf(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
      // Route only `text` through the V2 path; keep every other format on V1 so a Parquet probe
      // side (used by the broadcast-join test) is not pushed through the unsupported V2 scan.
      SQLConf.USE_V1_SOURCE_LIST.key -> "avro,csv,json,kafka,orc,parquet")(f)
  }

  test("native text read - lines to value column") {
    withTextConf {
      val df = spark.read.text(TEST_TEXT_PATH)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("native text read - count") {
    withTextConf {
      val df = spark.read.text(TEST_TEXT_PATH).selectExpr("count(*) as c")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("native text read - filter and projection") {
    withTextConf {
      val df = spark.read.text(TEST_TEXT_PATH).where("value like '%bar%'")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("native text read - preserves line order within a file") {
    withTempDir { dir =>
      val file = new File(dir, "ordered.txt")
      val lines = (1 to 500).map(i => s"row-$i")
      Files.write(file.toPath, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
      withTextConf {
        val df = spark.read.text(file.getAbsolutePath)
        // Assert the scan actually ran natively (a Spark fallback would also preserve order).
        checkSparkAnswerAndOperator(df, Seq(classOf[CometTextNativeScanExec]))
        // checkSparkAnswer sorts both sides, so assert order explicitly here.
        val got = df.collect().map(_.getString(0)).toSeq
        assert(got == lines, "native text scan must preserve within-file line order")
      }
    }
  }

  test("native text read - fallback for input_file_name") {
    withTextConf {
      val df = spark.read.text(TEST_TEXT_PATH).withColumn("f", input_file_name())
      checkSparkAnswerAndFallbackReason(
        df,
        "Native Text scan is not compatible with input_file_name")
    }
  }

  test("native text read - fallback when native exec is disabled") {
    withSQLConf(
      CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      checkSparkAnswerAndFallbackReason(
        spark.read.text(TEST_TEXT_PATH),
        s"Native Text scan requires ${CometConf.COMET_EXEC_ENABLED.key} to be enabled")
    }
  }

  test("native text read - wholetext") {
    withTextConf {
      val df = spark.read.option("wholetext", "true").text(TEST_TEXT_PATH)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("native text read - custom lineSep") {
    withTempDir { dir =>
      val file = new File(dir, "custom-linesep.txt")
      Files.write(file.toPath, "a|b|café|日本".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        val df = spark.read.option("lineSep", "|").text(file.getAbsolutePath)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("native text read - custom lineSep with trailing separator") {
    withTempDir { dir =>
      val file = new File(dir, "trailing-linesep.txt")
      // A trailing separator must not produce an extra empty row (matches Spark, SPARK-23577).
      Files.write(file.toPath, "a|b|c|".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        val df = spark.read.option("lineSep", "|").text(file.getAbsolutePath)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("native text read - non-ASCII multibyte custom lineSep") {
    withTempDir { dir =>
      val file = new File(dir, "jp-linesep.txt")
      // Japanese ideographic comma (U+3002, 3 UTF-8 bytes) as the record separator.
      Files.write(file.toPath, "a。café。日本".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        val df = spark.read.option("lineSep", "。").text(file.getAbsolutePath)
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("native text read - CRLF and CR line endings") {
    withTempDir { dir =>
      val file = new File(dir, "crlf.txt")
      // Mix of \r\n, \r, and \n; the default reader splits on all three.
      Files.write(file.toPath, "a\r\nb\rc\nd".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        checkSparkAnswerAndOperator(spark.read.text(file.getAbsolutePath))
      }
    }
  }

  test("native text read - invalid UTF-8 bytes match Spark on collect") {
    withTempDir { dir =>
      val file = new File(dir, "invalid-utf8.txt")
      // 0xFF is not valid UTF-8; both readers render it as the replacement char on collect.
      Files.write(file.toPath, Array[Byte]('a', 0xff.toByte, 'b', '\n', 'c'))
      withTextConf {
        checkSparkAnswerAndOperator(spark.read.text(file.getAbsolutePath))
      }
    }
  }

  test("native text read - wholetext count") {
    withTextConf {
      val df = spark.read
        .option("wholetext", "true")
        .text(TEST_TEXT_PATH)
        .selectExpr("count(*) as c")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("native text read - falls back for files split into byte ranges") {
    withTempDir { dir =>
      val file = new File(dir, "many-lines.txt")
      val content = (1 to 2000).map(i => s"line-$i-日本テスト").mkString("\n")
      Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
      withSQLConf(
        CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "",
        // Force the file to be split into several byte-range partitions.
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024") {
        val df = spark.read.text(file.getAbsolutePath)
        assert(df.rdd.getNumPartitions > 1, "test requires the file to actually split")
        checkSparkAnswerAndFallbackReason(
          df,
          "Comet native Text scan does not support files split into byte ranges")
      }
    }
  }

  test("native text read - empty file") {
    withTempDir { dir =>
      val file = new File(dir, "empty.txt")
      Files.write(file.toPath, Array.emptyByteArray)
      withTextConf {
        checkSparkAnswerAndOperator(spark.read.text(file.getAbsolutePath))
      }
    }
  }

  test("native text read - directory of multiple small files") {
    withTempDir { dir =>
      Files.write(new File(dir, "a.txt").toPath, "alpha\nbeta".getBytes(StandardCharsets.UTF_8))
      Files.write(new File(dir, "b.txt").toPath, "日本\ngamma".getBytes(StandardCharsets.UTF_8))
      Files.write(new File(dir, "c.txt").toPath, Array.emptyByteArray)
      withTextConf {
        checkSparkAnswerAndOperator(spark.read.text(dir.getAbsolutePath))
      }
    }
  }

  test("native text read - wholetext over a directory of files") {
    withTempDir { dir =>
      Files.write(
        new File(dir, "a.txt").toPath,
        "line1\nline2\n".getBytes(StandardCharsets.UTF_8))
      Files.write(new File(dir, "b.txt").toPath, "".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        // One row per file, including one empty row for the empty file.
        checkSparkAnswerAndOperator(
          spark.read.option("wholetext", "true").text(dir.getAbsolutePath))
      }
    }
  }

  test("native text read - falls back for large wholetext files") {
    withTempDir { dir =>
      val file = new File(dir, "big-wholetext.txt")
      Files.write(file.toPath, ("x" * 100).getBytes(StandardCharsets.UTF_8))
      withSQLConf(
        CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "",
        // Tiny limit so the 100-byte wholetext file exceeds the size cap.
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "16") {
        val df = spark.read.option("wholetext", "true").text(file.getAbsolutePath)
        checkSparkAnswerAndFallbackReason(
          df,
          "Comet native Text scan does not support large wholetext files")
      }
    }
  }

  test("native text read - strips a leading UTF-8 BOM in line mode") {
    withTempDir { dir =>
      val file = new File(dir, "bom.txt")
      val bom = Array[Byte](0xef.toByte, 0xbb.toByte, 0xbf.toByte)
      Files.write(file.toPath, bom ++ "hello\nworld".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        // Spark's line reader strips the BOM; native must match.
        checkSparkAnswerAndOperator(spark.read.text(file.getAbsolutePath))
      }
    }
  }

  test("native text read - fallback for ignoreCorruptFiles / ignoreMissingFiles") {
    withTempDir { dir =>
      val file = new File(dir, "a.txt")
      Files.write(file.toPath, "a\nb".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        checkSparkAnswerAndFallbackReason(
          spark.read.option("ignoreCorruptFiles", "true").text(file.getAbsolutePath),
          "Comet native Text scan does not support ignoreCorruptFiles")
        checkSparkAnswerAndFallbackReason(
          spark.read.option("ignoreMissingFiles", "true").text(file.getAbsolutePath),
          "Comet native Text scan does not support ignoreMissingFiles")
      }
    }
  }

  test("native text read - fallback for compressed (.gz) files") {
    withTempDir { dir =>
      val file = new File(dir, "data.txt.gz")
      val out = new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(file))
      out.write("a\nb\nc".getBytes(StandardCharsets.UTF_8))
      out.close()
      withTextConf {
        val df = spark.read.text(file.getAbsolutePath)
        checkSparkAnswerAndFallbackReason(df, "Comet does not support compressed text files")
      }
    }
  }

  test("native text read - fallback for .gzip/.zstd compressed files on Spark 4.1+") {
    // Spark 4.1+ decompresses the non-standard .gzip/.zstd extensions (via HadoopCodecStreams);
    // CompressionCodecFactory alone misses them, so the version-aware shim must catch them.
    assume(isSpark41Plus, "Spark 4.1+ treats .gzip/.zstd as compressed")
    withTempDir { dir =>
      val gzip = new File(dir, "data.txt.gzip")
      val out = new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(gzip))
      out.write("a\nb\nc".getBytes(StandardCharsets.UTF_8))
      out.close()
      withTextConf {
        checkSparkAnswerAndFallbackReason(
          spark.read.text(gzip.getAbsolutePath),
          "Comet does not support compressed text files")
      }
    }
  }

  test("native text read - fallback for line.maxlength") {
    withTempDir { dir =>
      val file = new File(dir, "lines.txt")
      Files.write(file.toPath, "ab\nabcdefghij\nxy".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        // As a per-read option the key reaches the scan's Hadoop conf (stripped), so Spark's line
        // reader skips lines >= this length; the native reader does not, so Comet must fall back.
        checkSparkAnswerAndFallbackReason(
          spark.read
            .option("mapreduce.input.linerecordreader.line.maxlength", "5")
            .text(file.getAbsolutePath),
          "mapreduce.input.linerecordreader.line.maxlength")
      }
    }
  }

  test("native text read - fallback for files larger than the size ceiling") {
    withTempDir { dir =>
      val file = new File(dir, "big.txt")
      Files.write(file.toPath, ("x\n" * 1000).getBytes(StandardCharsets.UTF_8))
      withSQLConf(
        CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "avro,csv,json,kafka,orc,parquet",
        // Tiny ceiling so the ~2KB file exceeds it.
        CometConf.COMET_SCAN_TEXT_MAX_FILE_SIZE.key -> "100") {
        checkSparkAnswerAndFallbackReason(
          spark.read.text(file.getAbsolutePath),
          "does not read files larger than")
      }
    }
  }

  test("native text read - partition column fallback: value stays native, select * falls back") {
    withTempDir { dir =>
      // Partitioned layout: <dir>/p=1/a.txt
      val part = new File(dir, "p=1")
      part.mkdirs()
      Files.write(new File(part, "a.txt").toPath, "x\ny".getBytes(StandardCharsets.UTF_8))
      withTextConf {
        // Selecting only `value` has no partition column -> stays native.
        checkSparkAnswerAndOperator(spark.read.text(dir.getAbsolutePath).select("value"))
        // Selecting the partition column `p` -> falls back to Spark.
        checkSparkAnswerAndFallbackReason(
          spark.read.text(dir.getAbsolutePath).selectExpr("value", "p"),
          "Comet does not support partition columns in native Text scans")
      }
    }
  }

  test("native text read - multiple unsplit files across separate partitions") {
    withTempDir { dir =>
      (1 to 6).foreach { i =>
        Files.write(
          new File(dir, s"f$i.txt").toPath,
          s"a$i\nb$i".getBytes(StandardCharsets.UTF_8))
      }
      withSQLConf(
        CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "avro,csv,json,kafka,orc,parquet",
        // A large open cost keeps each small file in its own partition, exercising
        // file_partitions[self.partition] beyond index 0.
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "8",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1073741824") {
        val df = spark.read.text(dir.getAbsolutePath)
        assert(df.rdd.getNumPartitions > 1, "test requires files in separate partitions")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("native text read - broadcast join with a text build side runs natively") {
    withTempDir { dir =>
      val lookup = new File(dir, "allow.txt")
      Files.write(lookup.toPath, "k1\nk2\nk3".getBytes(StandardCharsets.UTF_8))
      withSQLConf(
        CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "avro,csv,json,kafka,orc,parquet") {
        withParquetTable((1 to 100).map(i => (i, s"k${i % 5}")), "facts") {
          val lookupDf = spark.read.text(lookup.getAbsolutePath)
          val facts = spark.table("facts")
          // Small text build side broadcast-joined to a native Parquet probe side.
          val joined = facts.join(lookupDf.hint("broadcast"), facts("_2") === lookupDf("value"))
          checkSparkAnswerAndOperator(joined)
        }
      }
    }
  }
}
