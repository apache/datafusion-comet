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

package org.apache.comet

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class CometSparkSessionExtensionsSuite extends CometTestBase {
  test("unsupported Spark types") {
    Seq(
      NullType -> false,
      BooleanType -> true,
      ByteType -> true,
      ShortType -> true,
      IntegerType -> true,
      LongType -> true,
      FloatType -> true,
      DoubleType -> true,
      BinaryType -> true,
      StringType -> true,
      ArrayType(TimestampType) -> false,
      StructType(
        Seq(
          StructField("f1", DecimalType.SYSTEM_DEFAULT),
          StructField("f2", StringType))) -> false,
      MapType(keyType = LongType, valueType = DateType) -> false,
      StructType(Seq(StructField("f1", ByteType), StructField("f2", StringType))) -> false,
      MapType(keyType = IntegerType, valueType = BinaryType) -> false).foreach {
      case (dt, expected) =>
        assert(CometSparkSessionExtensions.isTypeSupported(dt) == expected)
    }
  }

  test("unsupported Spark schema") {
    Seq(
      Seq(StructField("f1", IntegerType), StructField("f2", BooleanType)) -> true,
      Seq(StructField("f1", IntegerType), StructField("f2", ArrayType(IntegerType))) -> false,
      Seq(
        StructField("f1", MapType(keyType = LongType, valueType = StringType)),
        StructField("f2", ArrayType(DoubleType))) -> false).foreach { case (schema, expected) =>
      assert(CometSparkSessionExtensions.isSchemaSupported(StructType(schema)) == expected)
    }
  }

  test("isCometEnabled") {
    val conf = new SQLConf

    conf.setConfString(CometConf.COMET_ENABLED.key, "false")
    assert(!CometSparkSessionExtensions.isCometEnabled(conf))

    // Since the native lib is probably already loaded due to previous tests, we reset it here
    NativeBase.setLoaded(false)

    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    val oldProperty = System.getProperty("os.name")
    System.setProperty("os.name", "foo")
    assert(!CometSparkSessionExtensions.isCometEnabled(conf))

    System.setProperty("os.name", oldProperty)

    conf.setConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION, true)
    assert(!CometSparkSessionExtensions.isCometEnabled(conf))

    // Restore the original state
    NativeBase.setLoaded(true)
  }

  test("Arrow properties") {
    NativeBase.setLoaded(false)
    NativeBase.load()

    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == "true")
    assert(System.getProperty(NativeBase.ARROW_NULL_CHECK_FOR_GET) == "false")

    System.setProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS, "false")
    NativeBase.setLoaded(false)
    NativeBase.load()
    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == "false")

    // Should not enable when debug mode is on
    System.clearProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS)
    SQLConf.get.setConfString(CometConf.COMET_DEBUG_ENABLED.key, "true")
    NativeBase.setLoaded(false)
    NativeBase.load()
    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == null)

    // Restore the original state
    NativeBase.setLoaded(true)
    SQLConf.get.setConfString(CometConf.COMET_DEBUG_ENABLED.key, "false")
  }

  def getBytesFromMib(mib: Long): Long = mib * 1024 * 1024

  test("Minimum Comet memory overhead") {
    val conf = new SparkConf()
    assert(
      CometSparkSessionExtensions
        .getCometMemoryOverhead(conf) == getBytesFromMib(384))
  }

  test("Comet memory overhead factor with executor memory") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.executor.memory", "16g")
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")

    assert(
      CometSparkSessionExtensions
        .getCometMemoryOverhead(sparkConf) == getBytesFromMib(8 * 1024))
  }

  test("Comet memory overhead factor with default executor memory") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")
    assert(
      CometSparkSessionExtensions
        .getCometMemoryOverhead(sparkConf) == getBytesFromMib(512))
  }

  test("Comet memory overhead") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "10g")
    assert(
      CometSparkSessionExtensions
        .getCometMemoryOverhead(sparkConf) == getBytesFromMib(1024 * 10))
  }
}
