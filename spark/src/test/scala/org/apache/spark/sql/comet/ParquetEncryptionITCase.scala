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

package org.apache.spark.sql.comet

import java.io.File
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import java.util.Base64
import org.junit.runner.RunWith
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatestplus.junit.JUnitRunner
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.{CometTestBase, QueryTest, SQLContext, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.comet.{CometConf, CometSparkSessionExtensions, IntegrationTestSuite}

/**
 * A integration test suite that tests parquet modular encryption usage.
 */
@RunWith(classOf[JUnitRunner])
@IntegrationTestSuite
class ParquetEncryptionITCase extends CometTestBase with SQLTestUtils {
  private val encoder = Base64.getEncoder
  private val footerKey =
    encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
  private val key2 = encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8))

  test("SPARK-34990: Write and read an encrypted parquet") {
    // https://github.com/apache/datafusion-comet/issues/1488
    assume(CometConf.COMET_NATIVE_SCAN_IMPL.get() != CometConf.SCAN_NATIVE_ICEBERG_COMPAT)

    import testImplicits._

    Seq("org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory").foreach {
      factoryClass =>
        withTempDir { dir =>
          withSQLConf(
            "parquet.crypto.factory.class" -> factoryClass,
            "parquet.encryption.kms.client.class" ->
              "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
            "parquet.encryption.key.list" ->
              s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

            // Make sure encryption works with multiple Parquet files
            val inputDF = spark
              .range(0, 2000)
              .map(i => (i, i.toString, i.toFloat))
              .repartition(10)
              .toDF("a", "b", "c")
            val parquetDir = new File(dir, "parquet").getCanonicalPath
            inputDF.write
              .option("parquet.encryption.column.keys", "key1: a, b; key2: c")
              .option("parquet.encryption.footer.key", "footerKey")
              .parquet(parquetDir)

            verifyParquetEncrypted(parquetDir)

            val parquetDF = spark.read.parquet(parquetDir)
            assert(parquetDF.inputFiles.nonEmpty)
            val readDataset = parquetDF.select("a", "b", "c")
            checkAnswer(readDataset, inputDF)
          }
        }
    }
  }

  test("SPARK-37117: Can't read files in Parquet encryption external key material mode") {
    // https://github.com/apache/datafusion-comet/issues/1488
    assume(CometConf.COMET_NATIVE_SCAN_IMPL.get() != CometConf.SCAN_NATIVE_ICEBERG_COMPAT)

    import testImplicits._

    Seq("org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory").foreach {
      factoryClass =>
        withTempDir { dir =>
          withSQLConf(
            "parquet.crypto.factory.class" -> factoryClass,
            "parquet.encryption.kms.client.class" ->
              "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
            "parquet.encryption.key.material.store.internally" -> "false",
            "parquet.encryption.key.list" ->
              s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

            val inputDF = spark
              .range(0, 2000)
              .map(i => (i, i.toString, i.toFloat))
              .repartition(10)
              .toDF("a", "b", "c")
            val parquetDir = new File(dir, "parquet").getCanonicalPath
            inputDF.write
              .option("parquet.encryption.column.keys", "key1: a, b; key2: c")
              .option("parquet.encryption.footer.key", "footerKey")
              .parquet(parquetDir)

            val parquetDF = spark.read.parquet(parquetDir)
            assert(parquetDF.inputFiles.nonEmpty)
            val readDataset = parquetDF.select("a", "b", "c")

            if (CometConf.COMET_ENABLED.get(conf)) {
              checkSparkAnswerAndOperator(readDataset)
            } else {
              checkAnswer(readDataset, inputDF)
            }
          }
        }
    }
  }

  protected override def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
    conf
  }

  protected override def createSparkSession: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[1]")
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq("true", "false").foreach { cometEnabled =>
      super.test(testName + s" Comet($cometEnabled)", testTags: _*) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled,
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "true") {
          testFun
        }
      }
    }
  }

  protected override def beforeAll(): Unit = {
    if (_spark == null) _spark = createSparkSession
    super.beforeAll()
  }

  private var _spark: SparkSession = _
  protected implicit override def spark: SparkSession = _spark
  protected implicit override def sqlContext: SQLContext = _spark.sqlContext

  /**
   * Verify that the directory contains an encrypted parquet in encrypted footer mode by means of
   * checking for all the parquet part files in the parquet directory that their magic string is
   * PARE, as defined in the spec:
   * https://github.com/apache/parquet-format/blob/master/Encryption.md#54-encrypted-footer-mode
   */
  private def verifyParquetEncrypted(parquetDir: String): Unit = {
    val parquetPartitionFiles = getListOfParquetFiles(new File(parquetDir))
    assert(parquetPartitionFiles.size >= 1)
    parquetPartitionFiles.foreach { parquetFile =>
      val magicString = "PARE"
      val magicStringLength = magicString.length()
      val byteArray = new Array[Byte](magicStringLength)
      val randomAccessFile = new RandomAccessFile(parquetFile, "r")
      try {
        randomAccessFile.read(byteArray, 0, magicStringLength)
      } finally {
        randomAccessFile.close()
      }
      val stringRead = new String(byteArray, StandardCharsets.UTF_8)
      assert(magicString == stringRead)
    }
  }

  private def getListOfParquetFiles(dir: File): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      file.getName.endsWith("parquet")
    }
  }
}
