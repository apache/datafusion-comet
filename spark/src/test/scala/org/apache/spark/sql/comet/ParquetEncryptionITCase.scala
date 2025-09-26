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

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.junit.runner.RunWith
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatestplus.junit.JUnitRunner

import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.{KeyToolkit, PropertiesDrivenCryptoFactory}
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.{CometTestBase, SQLContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

import org.apache.comet.{CometConf, IntegrationTestSuite}
import org.apache.comet.CometConf.{SCAN_NATIVE_COMET, SCAN_NATIVE_DATAFUSION, SCAN_NATIVE_ICEBERG_COMPAT}

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
  private val cryptoFactoryClass =
    "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"

  test("SPARK-34990: Write and read an encrypted parquet") {

    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        // Make sure encryption works with multiple Parquet files
        val inputDF = spark
          .range(0, 2000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(10)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

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

  test("SPARK-37117: Can't read files in Parquet encryption external key material mode") {

    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        KeyToolkit.KEY_MATERIAL_INTERNAL_PROPERTY_NAME -> "false", // default is true
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = spark
          .range(0, 2000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(10)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
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

  test("Plain text footer mode") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        PropertiesDrivenCryptoFactory.PLAINTEXT_FOOTER_PROPERTY_NAME -> "true", // default is false
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = spark
          .range(0, 1000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(5)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir)

        verifyParquetPlaintextFooter(parquetDir)

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

  test("Change encryption algorithm") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        // default is AES_GCM_V1
        PropertiesDrivenCryptoFactory.ENCRYPTION_ALGORITHM_PROPERTY_NAME -> "AES_GCM_CTR_V1",
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = spark
          .range(0, 1000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(5)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

        val parquetDF = spark.read.parquet(parquetDir)
        assert(parquetDF.inputFiles.nonEmpty)
        val readDataset = parquetDF.select("a", "b", "c")

        // native_datafusion and native_iceberg_compat fall back due to Arrow-rs
        // https://github.com/apache/arrow-rs/blob/main/parquet/src/file/metadata/parser.rs#L414
        if (CometConf.COMET_ENABLED.get(conf) && CometConf.COMET_NATIVE_SCAN_IMPL.get(
            conf) == SCAN_NATIVE_COMET) {
          checkSparkAnswerAndOperator(readDataset)
        } else {
          checkAnswer(readDataset, inputDF)
        }
      }
    }
  }

  test("Test double wrapping disabled") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME -> "false", // default is true
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = spark
          .range(0, 1000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(5)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

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

  test("Join between files with different encryption keys") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        // Write first file
        val inputDF1 = spark
          .range(0, 100)
          .map(i => (i, s"file1_${i}", i.toFloat))
          .toDF("id", "name", "value")
        val parquetDir1 = new File(dir, "parquet1").getCanonicalPath
        inputDF1.write
          .option(
            PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME,
            "key1: id, name, value")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir1)

        // Write second file using different column key
        val inputDF2 = spark
          .range(0, 100)
          .map(i => (i, s"file2_${i}", (i * 2).toFloat))
          .toDF("id", "description", "score")
        val parquetDir2 = new File(dir, "parquet2").getCanonicalPath
        inputDF2.write
          .option(
            PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME,
            "key2: id, description, score")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir2)

        // Now perform a join between the two files with different encryption keys
        // This tests that hadoopConf properties propagate correctly to each scan
        val parquetDF1 = spark.read.parquet(parquetDir1).alias("f1")
        val parquetDF2 = spark.read.parquet(parquetDir2).alias("f2")

        val joinedDF = parquetDF1
          .join(parquetDF2, parquetDF1("id") === parquetDF2("id"), "inner")
          .select(
            parquetDF1("id"),
            parquetDF1("name"),
            parquetDF2("description"),
            parquetDF2("score"))

        if (CometConf.COMET_ENABLED.get(conf)) {
          checkSparkAnswerAndOperator(joinedDF)
        } else {
          checkSparkAnswer(joinedDF)
        }
      }
    }
  }

  test("Test different key lengths") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
        KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        KeyToolkit.DATA_KEY_LENGTH_PROPERTY_NAME -> "256",
        KeyToolkit.KEK_LENGTH_PROPERTY_NAME -> "256",
        InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = spark
          .range(0, 1000)
          .map(i => (i, i.toString, i.toFloat))
          .repartition(5)
          .toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

        val parquetDF = spark.read.parquet(parquetDir)
        assert(parquetDF.inputFiles.nonEmpty)
        val readDataset = parquetDF.select("a", "b", "c")

        // native_datafusion and native_iceberg_compat fall back due to Arrow-rs not
        // supporting other key lengths
        if (CometConf.COMET_ENABLED.get(conf) && CometConf.COMET_NATIVE_SCAN_IMPL.get(
            conf) == SCAN_NATIVE_COMET) {
          checkSparkAnswerAndOperator(readDataset)
        } else {
          checkAnswer(readDataset, inputDF)
        }
      }
    }
  }

  protected override def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
    conf
  }

  protected override def createSparkSession: SparkSessionType = {
    createSparkSessionWithExtensions(sparkConf)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {

    Seq("true", "false").foreach { cometEnabled =>
      if (cometEnabled == "true") {
        Seq(SCAN_NATIVE_COMET, SCAN_NATIVE_DATAFUSION, SCAN_NATIVE_ICEBERG_COMPAT).foreach {
          scanImpl =>
            super.test(testName + s" Comet($cometEnabled)" + s" Scan($scanImpl)", testTags: _*) {
              withSQLConf(
                CometConf.COMET_ENABLED.key -> cometEnabled,
                CometConf.COMET_EXEC_ENABLED.key -> "true",
                SQLConf.ANSI_ENABLED.key -> "false",
                CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl) {
                testFun
              }
            }
        }
      } else {
        super.test(testName + s" Comet($cometEnabled)", testTags: _*) {
          withSQLConf(
            CometConf.COMET_ENABLED.key -> cometEnabled,
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> "false") {
            testFun
          }
        }
      }
    }
  }

  protected override def beforeAll(): Unit = {
    if (_spark == null) _spark = createSparkSession
    super.beforeAll()
  }

  private var _spark: SparkSessionType = _

  protected implicit override def spark: SparkSessionType = _spark

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

  /**
   * Verify that the directory contains an encrypted parquet in plaintext footer mode by means of
   * checking for all the parquet part files in the parquet directory that their magic string is
   * PAR1, as defined in the spec:
   * https://github.com/apache/parquet-format/blob/master/Encryption.md#55-plaintext-footer-mode
   */
  private def verifyParquetPlaintextFooter(parquetDir: String): Unit = {
    val parquetPartitionFiles = getListOfParquetFiles(new File(parquetDir))
    assert(parquetPartitionFiles.size >= 1)
    parquetPartitionFiles.foreach { parquetFile =>
      val magicString = "PAR1"
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
