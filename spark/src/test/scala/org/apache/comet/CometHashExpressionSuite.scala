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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, ParquetGenerator, SchemaGenOptions}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Test suite for Spark hash function compatibility between Spark and Comet.
 *
 * Native kernels are asserted for supported input shapes. Cases the native path declines
 * (`DecimalType` precision > 18, including nested, and `sha2` with a non-foldable `numBits`) must
 * stay in the Comet pipeline via the JVM codegen dispatcher.
 */
class CometHashExpressionSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  test("hash - boolean") {
    withTable("t") {
      sql("CREATE TABLE t(c BOOLEAN) USING parquet")
      sql("INSERT INTO t VALUES (true), (false), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - byte/tinyint") {
    withTable("t") {
      sql("CREATE TABLE t(c TINYINT) USING parquet")
      sql("INSERT INTO t VALUES (1), (0), (-1), (127), (-128), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - short/smallint") {
    withTable("t") {
      sql("CREATE TABLE t(c SMALLINT) USING parquet")
      sql("INSERT INTO t VALUES (1), (0), (-1), (32767), (-32768), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - int") {
    withTable("t") {
      sql("CREATE TABLE t(c INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (0), (-1), (2147483647), (-2147483648), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - long/bigint") {
    withTable("t") {
      sql("CREATE TABLE t(c BIGINT) USING parquet")
      sql(
        "INSERT INTO t VALUES (1), (0), (-1), (9223372036854775807), (-9223372036854775808), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - float") {
    withTable("t") {
      sql("CREATE TABLE t(c FLOAT) USING parquet")
      sql("INSERT INTO t VALUES (1.0), (0.0), (-0.0), (-1.0), (3.14159), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - double") {
    withTable("t") {
      sql("CREATE TABLE t(c DOUBLE) USING parquet")
      sql("INSERT INTO t VALUES (1.0), (0.0), (-0.0), (-1.0), (3.14159265358979), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - string") {
    withTable("t") {
      sql("CREATE TABLE t(c STRING) USING parquet")
      sql("INSERT INTO t VALUES ('hello'), (''), ('Spark SQL'), ('苹果手机'), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - binary") {
    withTable("t") {
      sql("CREATE TABLE t(c BINARY) USING parquet")
      sql("INSERT INTO t VALUES (X''), (X'00'), (X'0102030405'), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - date") {
    withTable("t") {
      sql("CREATE TABLE t(c DATE) USING parquet")
      sql(
        "INSERT INTO t VALUES (DATE '2023-01-01'), (DATE '1970-01-01'), (DATE '2000-12-31'), (null)")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - timestamp") {
    withTable("t") {
      sql("CREATE TABLE t(c TIMESTAMP) USING parquet")
      sql("""INSERT INTO t VALUES
            (TIMESTAMP '2023-01-01 12:00:00'),
            (TIMESTAMP '1970-01-01 00:00:00'),
            (TIMESTAMP '2000-12-31 23:59:59'),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
    }
  }

  test("hash - decimal (precision <= 18)") {
    Seq((10, 2), (18, 0), (18, 10)).foreach { case (precision, scale) =>
      withTable("t") {
        sql(s"CREATE TABLE t(c DECIMAL($precision, $scale)) USING parquet")
        sql("INSERT INTO t VALUES (1.23), (-1.23), (0.0), (null)")
        assertNativeHash("SELECT c, hash(c), xxhash64(c) FROM t ORDER BY c")
      }
    }
  }

  test("hash - decimal (precision > 18)") {
    Seq((20, 2), (38, 10)).foreach { case (precision, scale) =>
      withTable("t") {
        sql(s"CREATE TABLE t(c DECIMAL($precision, $scale)) USING parquet")
        sql("INSERT INTO t VALUES (1.23), (-1.23), (0.0), (null)")
        assertCodegenRan {
          checkSparkAnswerAndOperator("SELECT c, hash(c), xxhash64(c) FROM t ORDER BY c")
        }
      }
    }
  }

  test("hash - array of decimal (precision > 18) routes through the codegen dispatcher") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<DECIMAL(20, 2)>) USING parquet")
      sql("INSERT INTO t VALUES (array(1.23, 2.34)), (null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator("SELECT c, hash(c), xxhash64(c) FROM t")
      }
    }
  }

  test("hash - struct with decimal (precision > 18) routes through the codegen dispatcher") {
    withTable("t") {
      sql("CREATE TABLE t(c STRUCT<a: INT, b: DECIMAL(20, 2)>) USING parquet")
      sql("INSERT INTO t VALUES (named_struct('a', 1, 'b', 1.23)), (null)")
      assertCodegenRan {
        checkSparkAnswerAndOperator("SELECT c, hash(c), xxhash64(c) FROM t")
      }
    }
  }

  test("hash - map with decimal (precision > 18) value routes through the codegen dispatcher") {
    withSQLConf("spark.sql.legacy.allowHashOnMapType" -> "true") {
      withTable("t") {
        sql("CREATE TABLE t(c MAP<STRING, DECIMAL(20, 2)>) USING parquet")
        sql("INSERT INTO t VALUES (map('a', 1.23)), (null)")
        assertCodegenRan {
          checkSparkAnswerAndOperator("SELECT c, hash(c), xxhash64(c) FROM t")
        }
      }
    }
  }

  test("hash - wide decimal falls back when codegen dispatcher is disabled") {
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withTable("t") {
        sql("CREATE TABLE t(c DECIMAL(20, 2)) USING parquet")
        sql("INSERT INTO t VALUES (1.23), (-1.23), (0.0), (null)")
        checkSparkAnswerAndFallbackReasons(
          "SELECT c, hash(c), xxhash64(c) FROM t ORDER BY c",
          Set(
            s"hash: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false",
            s"xxhash64: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false"))
      }
    }
  }

  test("sha2 - non-foldable numBits routes through the codegen dispatcher") {
    withTable("t") {
      sql("CREATE TABLE t(payload STRING, num_bits INT) USING parquet")
      sql("""INSERT INTO t VALUES
            ('hello', 0),
            ('hello', 224),
            ('hello', 256),
            ('hello', 384),
            ('hello', 512),
            ('hello', 128),
            ('hello', -1),
            (NULL, 256),
            ('hello', NULL)""")
      assertCodegenRan {
        checkSparkAnswerAndOperator("SELECT sha2(payload, num_bits) FROM t")
      }
    }
  }

  test("sha2 - literal numBits stays native") {
    withTable("t") {
      sql("CREATE TABLE t(payload STRING) USING parquet")
      sql("INSERT INTO t VALUES ('hello'), (''), (NULL)")
      assertNativeHash("SELECT sha2(payload, 256) FROM t")
    }
  }

  test("sha2 - non-foldable numBits falls back when codegen dispatcher is disabled") {
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withTable("t") {
        sql("CREATE TABLE t(payload STRING, num_bits INT) USING parquet")
        sql("INSERT INTO t VALUES ('hello', 256), (NULL, 256), ('hello', NULL)")
        checkSparkAnswerAndFallbackReason(
          "SELECT sha2(payload, num_bits) FROM t",
          s"sha2: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false")
      }
    }
  }

  test("hash - array of integers") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<INT>) USING parquet")
      sql("""INSERT INTO t VALUES
            (array(1, 2, 3)),
            (array(-1, 0, 1)),
            (array()),
            (null),
            (array(null)),
            (array(1, null, 3))""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - array of strings") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<STRING>) USING parquet")
      sql("""INSERT INTO t VALUES
            (array('hello', 'world')),
            (array('Spark', 'SQL')),
            (array('')),
            (array()),
            (null),
            (array(null)),
            (array('a', null, 'b'))""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - array of doubles") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<DOUBLE>) USING parquet")
      sql("""INSERT INTO t VALUES
            (array(1.0, 2.0, 3.0)),
            (array(-1.0, 0.0, 1.0)),
            (array()),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - nested array (array of arrays)") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<ARRAY<INT>>) USING parquet")
      sql("""INSERT INTO t VALUES
            (array(array(1, 2), array(3, 4))),
            (array(array(), array(1))),
            (array()),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - struct") {
    withTable("t") {
      sql("CREATE TABLE t(c STRUCT<a: INT, b: STRING>) USING parquet")
      sql("""INSERT INTO t VALUES
            (named_struct('a', 1, 'b', 'hello')),
            (named_struct('a', -1, 'b', '')),
            (named_struct('a', null, 'b', 'test')),
            (named_struct('a', 42, 'b', null)),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - nested struct") {
    withTable("t") {
      sql("CREATE TABLE t(c STRUCT<a: INT, b: STRUCT<x: STRING, y: DOUBLE>>) USING parquet")
      sql("""INSERT INTO t VALUES
            (named_struct('a', 1, 'b', named_struct('x', 'hello', 'y', 3.14))),
            (named_struct('a', 2, 'b', named_struct('x', '', 'y', 0.0))),
            (named_struct('a', 3, 'b', null)),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - null struct with a required child field") {
    // `c` is nullable but its child is REQUIRED, so Spark writes
    // `optional group c { required int32 a; }`. On read the child leaf has nowhere to record a
    // null of its own, so its buffer holds a value at exactly the rows where the struct is null.
    // That is the shape where the parent's null mask has to reach the children; a struct built in
    // the plan, or one whose child is also nullable, has null children there and hides the bug.
    withTempPath { dir =>
      val schema = StructType(
        Seq(
          StructField(
            "c",
            StructType(Seq(StructField("a", IntegerType, nullable = false))),
            nullable = true)))
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val rows = Seq(Row(Row(1)), Row(null), Row(Row(3)), Row(null))
        spark
          .createDataFrame(spark.sparkContext.parallelize(rows), schema)
          .coalesce(1)
          .write
          .parquet(dir.toString)
      }
      spark.read.parquet(dir.toString).createOrReplaceTempView("null_struct_t")
      checkSparkAnswerAndOperator("SELECT hash(c), xxhash64(c) FROM null_struct_t ORDER BY 1, 2")
    }
  }

  test("hash - null struct whose child is itself a struct") {
    // The union has to recurse: the outer struct's nulls reach the inner struct, whose own
    // children are required and so carry values under the null.
    withTempPath { dir =>
      val inner = StructType(Seq(StructField("x", IntegerType, nullable = false)))
      val schema = StructType(
        Seq(
          StructField(
            "c",
            StructType(Seq(StructField("b", inner, nullable = false))),
            nullable = true)))
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val rows = Seq(Row(Row(Row(1))), Row(null), Row(Row(Row(3))), Row(null))
        spark
          .createDataFrame(spark.sparkContext.parallelize(rows), schema)
          .coalesce(1)
          .write
          .parquet(dir.toString)
      }
      spark.read.parquet(dir.toString).createOrReplaceTempView("null_nested_struct_t")
      checkSparkAnswerAndOperator(
        "SELECT hash(c), xxhash64(c) FROM null_nested_struct_t ORDER BY 1, 2")
    }
  }

  test("hash - list element wrapping a null struct with a required child field") {
    // The per-element path in `hash_list_array!`, which #5567 made usable as a shuffle key.
    //
    // Wrapping the struct rather than using it as the element directly is what makes this
    // reachable: `array(named_struct('tag', 1, 'b', c))` produces an element that is itself
    // valid, so it is copied rather than rebuilt, and the null `c` inside it keeps the child
    // values Parquet wrote under it. Using `c` as the element directly does not reproduce,
    // because a null element is rebuilt on the way in and the hidden values go with it.
    //
    // So this covers a null struct nested inside a valid element. The unit test in murmur3.rs
    // covers the complementary shape, where the element itself is the null struct.
    withTempPath { dir =>
      val schema = StructType(
        Seq(
          StructField(
            "c",
            StructType(Seq(StructField("a", IntegerType, nullable = false))),
            nullable = true)))
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val rows = Seq(Row(Row(1)), Row(null), Row(Row(3)), Row(null))
        spark
          .createDataFrame(spark.sparkContext.parallelize(rows), schema)
          .coalesce(1)
          .write
          .parquet(dir.toString)
      }
      spark.read.parquet(dir.toString).createOrReplaceTempView("wrapped_null_struct_t")

      checkSparkAnswerAndOperator("""
        SELECT
          hash(array(named_struct('tag', 1, 'b', c))),
          xxhash64(array(named_struct('tag', 1, 'b', c)))
        FROM wrapped_null_struct_t ORDER BY 1, 2""")

      // Two elements, so the hash of the second chains onto the first.
      checkSparkAnswerAndOperator("""
        SELECT
          hash(array(named_struct('tag', 1, 'b', c), named_struct('tag', 2, 'b', c))),
          xxhash64(array(named_struct('tag', 1, 'b', c), named_struct('tag', 2, 'b', c)))
        FROM wrapped_null_struct_t ORDER BY 1, 2""")
    }
  }

  test("hash - struct with array field") {
    withTable("t") {
      sql("CREATE TABLE t(c STRUCT<a: INT, b: ARRAY<STRING>>) USING parquet")
      sql("""INSERT INTO t VALUES
            (named_struct('a', 1, 'b', array('x', 'y'))),
            (named_struct('a', 2, 'b', array())),
            (named_struct('a', 3, 'b', null)),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - array of structs") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<STRUCT<a: INT, b: STRING>>) USING parquet")
      sql("""INSERT INTO t VALUES
            (array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))),
            (array(named_struct('a', 3, 'b', ''))),
            (array()),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - map") {
    // Spark prohibits hash on map types by default, enable legacy mode for testing
    withSQLConf("spark.sql.legacy.allowHashOnMapType" -> "true") {
      withTable("t") {
        sql("CREATE TABLE t(c MAP<STRING, INT>) USING parquet")
        sql("""INSERT INTO t VALUES
              (map('a', 1, 'b', 2)),
              (map('x', -1)),
              (map()),
              (null)""")
        checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
      }
    }
  }

  test("hash - map with complex value type") {
    // Spark prohibits hash on map types by default, enable legacy mode for testing
    withSQLConf("spark.sql.legacy.allowHashOnMapType" -> "true") {
      withTable("t") {
        sql("CREATE TABLE t(c MAP<STRING, ARRAY<INT>>) USING parquet")
        sql("""INSERT INTO t VALUES
              (map('a', array(1, 2), 'b', array(3))),
              (map('x', array())),
              (map()),
              (null)""")
        checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
      }
    }
  }

  test("hash - multiple primitive columns") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b STRING, c DOUBLE) USING parquet")
      sql("""INSERT INTO t VALUES
            (1, 'hello', 3.14),
            (2, '', 0.0),
            (null, null, null),
            (-1, 'test', -1.5)""")
      checkSparkAnswerAndOperator(
        "SELECT hash(a, b, c), hash(c, b, a), hash(a), hash(b), hash(c) FROM t")
    }
  }

  test("hash - multiple columns with arrays") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b ARRAY<INT>, c STRING) USING parquet")
      sql("""INSERT INTO t VALUES
            (1, array(1, 2, 3), 'hello'),
            (2, array(), ''),
            (null, null, null),
            (3, array(-1, 0, 1), 'test')""")
      checkSparkAnswerAndOperator("SELECT hash(a, b, c), hash(b), hash(a, c) FROM t")
    }
  }

  test("hash - multiple columns with structs") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b STRUCT<x: INT, y: STRING>) USING parquet")
      sql("""INSERT INTO t VALUES
            (1, named_struct('x', 10, 'y', 'hello')),
            (2, named_struct('x', 20, 'y', '')),
            (null, null),
            (3, named_struct('x', null, 'y', 'test'))""")
      checkSparkAnswerAndOperator("SELECT hash(a, b), hash(b, a), hash(b) FROM t")
    }
  }

  test("hash - empty strings and arrays") {
    withTable("t") {
      sql("CREATE TABLE t(s STRING, a ARRAY<INT>) USING parquet")
      sql("INSERT INTO t VALUES ('', array()), ('a', array(1))")
      checkSparkAnswerAndOperator("SELECT hash(s), hash(a), hash(s, a) FROM t")
    }
  }

  test("hash - all nulls") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b STRING, c ARRAY<INT>) USING parquet")
      sql("INSERT INTO t VALUES (null, null, null)")
      checkSparkAnswerAndOperator("SELECT hash(a), hash(b), hash(c), hash(a, b, c) FROM t")
    }
  }

  test("hash - with custom seed") {
    withTable("t") {
      sql("CREATE TABLE t(c INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (null)")
      // hash() with seed 42 (default) and seed 0
      checkSparkAnswerAndOperator(
        "SELECT hash(c), hash(c, 0), hash(c, 42), hash(c, -1) FROM t ORDER BY c")
    }
  }

  test("hash - large array") {
    withTable("t") {
      sql("CREATE TABLE t(c ARRAY<INT>) USING parquet")
      // Create an array with 1000 elements
      val largeArray = (1 to 1000).mkString("array(", ", ", ")")
      sql(s"INSERT INTO t VALUES ($largeArray)")
      checkSparkAnswerAndOperator("SELECT hash(c) FROM t")
    }
  }

  test("hash - deeply nested structure") {
    withTable("t") {
      sql("""CREATE TABLE t(c STRUCT<
              a: INT,
              b: STRUCT<
                x: STRING,
                y: ARRAY<STRUCT<p: INT, q: STRING>>
              >
            >) USING parquet""")
      sql("""INSERT INTO t VALUES
            (named_struct('a', 1, 'b', named_struct('x', 'hello', 'y',
              array(named_struct('p', 10, 'q', 'foo'), named_struct('p', 20, 'q', 'bar'))))),
            (null)""")
      checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
    }
  }

  test("hash - with dictionary encoding") {
    Seq(true, false).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        withTable("t") {
          sql("CREATE TABLE t(c STRING) USING parquet")
          // Repeated values to trigger dictionary encoding
          sql("INSERT INTO t VALUES ('a'), ('b'), ('a'), ('b'), ('a'), ('c'), (null)")
          checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t ORDER BY c")
        }
      }
    }
  }

  test("hash - array with dictionary encoding") {
    Seq(true, false).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        withTable("t") {
          sql("CREATE TABLE t(c ARRAY<STRING>) USING parquet")
          sql("""INSERT INTO t VALUES
                (array('a', 'b')),
                (array('a', 'b')),
                (array('c')),
                (null)""")
          checkSparkAnswerAndOperator("SELECT c, hash(c) FROM t")
        }
      }
    }
  }

  test("hash - fuzz test") {
    val r = new Random(42)
    val options = SchemaGenOptions(generateStruct = true)
    val schema = FuzzDataGenerator.generateNestedSchema(r, 50, 1, 2, options)
    withTempPath { filename =>
      ParquetGenerator.makeParquetFile(
        r,
        spark,
        filename.toString,
        schema,
        1000,
        DataGenOptions())
      spark.read.parquet(filename.toString).createOrReplaceTempView("t1")
      for (col <- schema.fields) {
        val name = col.name
        checkSparkAnswer(s"select $name, hash($name) from t1 order by $name")
      }
    }
  }

  private def assertNativeHash(query: String): Unit = {
    CometScalaUDFCodegen.resetStats()
    checkSparkAnswerAndOperator(query)
    assert(
      CometScalaUDFCodegen.stats().totalLookups == 0,
      s"expected native hash execution for $query, got ${CometScalaUDFCodegen.stats()}")
  }
}
