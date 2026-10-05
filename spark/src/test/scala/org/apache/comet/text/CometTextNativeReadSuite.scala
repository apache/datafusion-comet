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
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometTextNativeReadSuite extends CometTestBase {
  private val TEST_TEXT_PATH = "src/test/resources/test-data/text-test-1.txt"

  private def withTextConf(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_TEXT_V2_NATIVE_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "")(f)
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
        val got = spark.read.text(file.getAbsolutePath).collect().map(_.getString(0)).toSeq
        // checkSparkAnswer sorts both sides, so assert order explicitly here.
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
}
