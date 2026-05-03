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

package org.apache.comet.exec

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, StringType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

class CometNativeReaderSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT).foreach(scan =>
      super.test(s"$testName - $scan", testTags: _*) {
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan) {
          testFun
        }
      })
  }

  test("native reader case sensitivity") {
    withTempPath { path =>
      spark.range(10).toDF("a").write.parquet(path.toString)
      Seq(true, false).foreach { caseSensitive =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
          val tbl = s"case_sensitivity_${caseSensitive}_${System.currentTimeMillis()}"
          sql(s"create table $tbl (A long) using parquet options (path '" + path + "')")
          val df = sql(s"select A from $tbl")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("native reader duplicate fields in case-insensitive mode") {
    withTempPath { path =>
      // Write parquet with columns A, B, b (B and b are duplicates case-insensitively)
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        spark
          .range(5)
          .selectExpr("id as A", "id as B", "id as b")
          .write
          .mode("overwrite")
          .parquet(path.toString)
      }
      val tbl = s"dup_fields_${System.currentTimeMillis()}"
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        sql(s"create table $tbl (A long, B long) using parquet options (path '${path}')")
      }
      // In case-insensitive mode, selecting B should fail because both B and b match
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        val e = intercept[Exception] {
          sql(s"select B from $tbl").collect()
        }
        assert(
          e.getMessage.contains("duplicate field") ||
            e.getMessage.contains("Found duplicate field") ||
            (e.getCause != null && e.getCause.getMessage.contains("duplicate field")) ||
            (e.getCause != null && e.getCause.getMessage.contains("Found duplicate field")),
          s"Expected duplicate field error, got: ${e.getMessage}")
      }
      // In case-sensitive mode, selecting B should work fine
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val df = sql(s"select A from $tbl")
        assert(df.collect().length == 5)
      }
      sql(s"drop table if exists $tbl")
    }
  }

  test("native reader - read simple STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('firstName', 'John', 'lastName', 'Doe', 'age', 35) as personal_info union all
        |select named_struct('firstName', 'Jane', 'lastName', 'Doe', 'age', 40) as personal_info
        |""".stripMargin,
      "select personal_info.* from tbl")
  }

  test("native reader - read simple ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(1, 2, 3) as arr union all
        |select array(2, 3, 4) as arr
        |""".stripMargin,
      "select arr from tbl")
  }

  test("native reader - read STRUCT of ARRAY fields") {
    testSingleLineQuery(
      """
        |select named_struct('col', arr) c0 from
        |(
        |  select array(1, 2, 3) as arr union all
        |  select array(2, 3, 4) as arr
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(arr0, arr1) c0 from
        |(
        |  select array(1, 2, 3) as arr0, array(2, 3, 4) as arr1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of STRUCT fields") {
    testSingleLineQuery(
      """
        |select array(str0, str1) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0, named_struct('a', 2, 'b', 'w') str1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('a', str0, 'b', str1) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0, named_struct('c', 2, 'd', 'w') str1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of ARRAY of STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('a', array(str0, str1), 'b', array(str2, str3)) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0,
        |         named_struct('a', 2, 'b', 'w') str1,
        |         named_struct('x', 3, 'y', 'a') str2,
        |         named_struct('x', 4, 'y', 'c') str3
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of STRUCT of ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(named_struct('a', a0, 'b', a1), named_struct('a', a2, 'b', a3)) c0 from
        |(
        |  select array(1, 2, 3) a0,
        |         array(2, 3, 4) a1,
        |         array(3, 4, 5) a2,
        |         array(4, 5, 6) a3
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read simple MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', 1) as c0 union all
        |select map('b', 2)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value ARRAY fields") {
    testSingleLineQuery(
      """
        |select map('a', array(1), 'c', array(3)) as c0 union all
        |select map('b', array(2))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value STRUCT fields") {
    testSingleLineQuery(
      """
        |select map('a', named_struct('f0', 0, 'f1', 'foo'), 'b', named_struct('f0', 1, 'f1', 'bar')) as c0 union all
        |select map('c', named_struct('f2', 0, 'f1', 'baz')) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', map('a1', 1, 'b1', 2), 'b', map('a2', 2, 'b2', 3)) as c0 union all
        |select map('c', map('a3', 3, 'b3', 4))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of MAP fields") {
    testSingleLineQuery(
      """
        |select named_struct('m0', map('a', 1)) as c0 union all
        |select named_struct('m1', map('b', 2))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of MAP fields") {
    testSingleLineQuery(
      """
        |select array(map('a', 1), map('b', 2)) as c0 union all
        |select array(map('c', 3))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of MAP of ARRAY value fields") {
    testSingleLineQuery(
      """
        |select array(map('a', array(1, 2, 3), 'b', array(2, 3, 4)), map('c', array(4, 5, 6), 'd', array(7, 8, 9))) as c0 union all
        |select array(map('x', array(1, 2, 3), 'y', array(2, 3, 4)), map('c', array(4, 5, 6), 'z', array(7, 8, 9)))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of MAP of STRUCT value fields") {
    testSingleLineQuery(
      """
        |select named_struct('m0', map('a', named_struct('f0', 1)), 'm1', map('b', named_struct('f1', 1))) as c0 union all
        |select named_struct('m0', map('c', named_struct('f2', 1)), 'm1', map('d', named_struct('f3', 1))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of ARRAY of MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', array(map(1, 'a', 2, 'b'), map(1, 'a', 2, 'b'))) as c0 union all
        |select map('b', array(map(1, 'a', 2, 'b'), map(1, 'a', 2, 'b'))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of STRUCT of MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', named_struct('f0', map(1, 'b')), 'b', named_struct('f0', map(1, 'b'))) as c0 union all
        |select map('c', named_struct('f0', map(1, 'b'))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }
  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - second field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].b col0 from tbl")
  }

  test("native reader - read a STRUCT subfield - field from second") {
    testSingleLineQuery(
      """
        |select 1 a, named_struct('a', 1, 'b', 'n') c0
        |""".stripMargin,
      "select c0.b from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - field from first") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].b, c0[0].c from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - reverse fields") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].c, c0[0].b, c0[0].a from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - skip field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].c from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - duplicate first field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].a from tbl")
  }

  test("native reader - select nested field from a complex map[struct, struct] using map_keys") {
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |   select named_struct('a', cast(1 as long), 'b', cast(2 as long), 'c', cast(3 as long)) str0,
        |          named_struct('x', cast(8 as long), 'y', cast(9 as long), 'z', cast(0 as long)) str1 union all
        |   select named_struct('a', cast(3 as long), 'b', cast(4 as long), 'c', cast(5 as long)) str0,
        |          named_struct('x', cast(6 as long), 'y', cast(7 as long), 'z', cast(8 as long)) str1
        | )
        |""".stripMargin,
      "select map_keys(c0).b from tbl")
  }

  test(
    "native reader - select nested field from a complex map[struct, struct] using map_values") {
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |   select named_struct('a', cast(1 as long), 'b', cast(2 as long), 'c', cast(3 as long)) str0,
        |          named_struct('x', cast(8 as long), 'y', cast(9 as long), 'z', cast(0 as long)) str1 union all
        |   select named_struct('a', cast(3 as long), 'b', cast(4 as long), 'c', cast(5 as long)) str0,
        |          named_struct('x', cast(6 as long), 'y', cast(7 as long), 'z', cast(8 as long)) str1 union all
        |   select named_struct('a', cast(31 as long), 'b', cast(41 as long), 'c', cast(51 as long)), null
        | )
        |""".stripMargin,
      "select map_values(c0).y from tbl")
  }

  test("native reader - select struct field with user defined schema") {
    assume(!isSpark41Plus, "https://github.com/apache/datafusion-comet/issues/4098")
    // extract existing A column
    var readSchema = new StructType().add(
      "c0",
      new StructType()
        .add("a", IntegerType, nullable = true),
      nullable = true)

    testSingleLineQuery(
      """
        | select named_struct('a', 0, 'b', 'xyz') c0
        |""".stripMargin,
      "select * from tbl",
      readSchema = Some(readSchema))

    // extract existing A column, nonexisting X
    readSchema = new StructType().add(
      "c0",
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("x", StringType, nullable = true),
      nullable = true)

    testSingleLineQuery(
      """
        | select named_struct('a', 0, 'b', 'xyz') c0
        |""".stripMargin,
      "select * from tbl",
      readSchema = Some(readSchema))

    // extract nonexisting X, Y columns
    readSchema = new StructType().add(
      "c0",
      new StructType()
        .add("y", IntegerType, nullable = true)
        .add("x", StringType, nullable = true),
      nullable = true)

    testSingleLineQuery(
      """
        | select named_struct('a', 0, 'b', 'xyz') c0
        |""".stripMargin,
      "select * from tbl",
      readSchema = Some(readSchema))
  }

  test("native reader - extract map by key") {
    // existing key
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |    select 'key0' str0, named_struct('a', 1, 'b', 'str') str1
        | )
        |""".stripMargin,
      "select c0['key0'] from tbl")

    // existing key, existing struct subfield
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |    select 'key0' str0, named_struct('a', 1, 'b', 'str') str1
        | )
        |""".stripMargin,
      "select c0['key0'].b from tbl")

    // nonexisting key
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |    select 'key0' str0, named_struct('a', 1, 'b', 'str') str1
        | )
        |""".stripMargin,
      "select c0['key1'] from tbl")

    // nonexisting key, existing struct subfield
    testSingleLineQuery(
      """
        | select map(str0, str1) c0 from
        | (
        |    select 'key0' str0, named_struct('a', 1, 'b', 'str') str1
        | )
        |""".stripMargin,
      "select c0['key1'].b from tbl")
  }

  test("native reader - support ARRAY literal INT fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(1, 2, null, 3, null) from tbl")
  }

  test("native reader - support ARRAY literal BOOL fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(true, null, false, null) from tbl")
  }

  test("native reader - support ARRAY literal NULL fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(null) from tbl")
  }

  test("native reader - support empty ARRAY literal") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array() from tbl")
  }

  test("native reader - support ARRAY literal BYTE fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as byte), cast(2 as byte), null, cast(3 as byte), null) from tbl")
  }

  test("native reader - support ARRAY literal SHORT fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as short), cast(2 as short), null, cast(3 as short), null) from tbl")
  }

  test("native reader - support ARRAY literal DATE fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(CAST('2024-01-01' AS DATE), CAST('2024-02-01' AS DATE), null, CAST('2024-03-01' AS DATE), null) from tbl")
  }

  test("native reader - support ARRAY literal LONG fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as bigint), cast(2 as bigint), null, cast(3 as bigint), null) from tbl")
  }

  test("native reader - support ARRAY literal TIMESTAMP fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(CAST('2024-01-01 10:00:00' AS TIMESTAMP), CAST('2024-01-02 10:00:00' AS TIMESTAMP), null, CAST('2024-01-03 10:00:00' AS TIMESTAMP), null) from tbl")
  }

  test("native reader - support ARRAY literal TIMESTAMP TZ fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(CAST('2024-01-01 10:00:00' AS TIMESTAMP_NTZ), CAST('2024-01-02 10:00:00' AS TIMESTAMP_NTZ), null, CAST('2024-01-03 10:00:00' AS TIMESTAMP_NTZ), null) from tbl")
  }

  test("native reader - support ARRAY literal FLOAT fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as float), cast(2 as float), null, cast(3 as float), null) from tbl")
  }

  test("native reader - support ARRAY literal DOUBLE fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as double), cast(2 as double), null, cast(3 as double), null) from tbl")
  }

  test("native reader - support ARRAY literal STRING fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array('a', 'bc', null, 'def', null) from tbl")
  }

  test("native reader - support ARRAY literal DECIMAL fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast(1 as decimal(10, 2)), cast(2.5 as decimal(10, 2)), null, cast(3.75 as decimal(10, 2)), null) from tbl")
  }

  test("native reader - support ARRAY literal BINARY fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(cast('a' as binary), cast('bc' as binary), null, cast('def' as binary), null) from tbl")
  }

  test("SPARK-18053: ARRAY equality is broken") {
    withTable("array_tbl") {
      spark.range(10).select(array(col("id")).as("arr")).write.saveAsTable("array_tbl")
      assert(sql("SELECT * FROM array_tbl where arr = ARRAY(1L)").count == 1)
    }
  }

  test("native reader - support ARRAY literal nested ARRAY fields") {
    testSingleLineQuery(
      """
        |select 1 a
        |""".stripMargin,
      "select array(array(1, 2, null), array(), array(10), null, array(null)) from tbl")
  }

  // Regression test found during DataFusion 53 upgrade (PR #3629).
  // Spark's SchemaPruningSuite tests (e.g. "select a single complex field array
  // and in clause", "select explode of nested field of array of struct") were
  // failing because wrap_all_type_mismatches in Comet's schema adapter looked up
  // the logical field by column index instead of by name. Filter expressions
  // built against the pruned required_schema had "friends" at index 0, but the
  // full logical_file_schema had "id: Int32" at index 0.
  test("native reader - nested schema pruning with array of struct and filter") {
    testSingleLineQuery(
      """
        |select
        |  0 as id,
        |  named_struct('first', 'Jane', 'middle', 'X.', 'last', 'Doe') as name,
        |  '123 Main Street' as address,
        |  1 as pets,
        |  array(
        |    named_struct('first', 'Susan', 'middle', 'Z.', 'last', 'Smith')
        |  ) as friends
        |union all
        |select
        |  1 as id,
        |  named_struct('first', 'John', 'middle', 'Y.', 'last', 'Doe') as name,
        |  '321 Wall Street' as address,
        |  3 as pets,
        |  array(
        |    named_struct('first', 'Alice', 'middle', 'A.', 'last', 'Jones')
        |  ) as friends
        |""".stripMargin,
      "select friends.middle from tbl where friends.first[0] = 'Susan'")
  }

  // SPARK-39393: bare "repeated int32" (protobuf-style, no wrapping list group)
  // should be readable without crashing on missing def levels.
  // SPARK-39393: Parquet does not support predicate pushdown on repeated columns.
  // A bare "repeated int32 f" (protobuf-style, no wrapping LIST group) must not
  // have IsNotNull pushed into the Parquet reader. Comet filters these out in
  // CometScanExec.supportedDataFilters so the predicate is evaluated after
  // reading. Without that, DataFusion's list predicate pushdown would push
  // IsNotNull as a RowFilter, triggering an arrow-rs ListArrayReader crash.
  test("native reader - read bare repeated primitive field") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "protobuf-parquet").toString
      val schema =
        """message protobuf_style {
          |  repeated int32 f;
          |}
        """.stripMargin

      writeDirect(
        path,
        schema,
        { rc =>
          rc.startMessage()
          rc.startField("f", 0)
          rc.addInteger(1)
          rc.addInteger(2)
          rc.endField("f", 0)
          rc.endMessage()
        })

      // Read without filter
      checkAnswer(spark.read.parquet(dir.getCanonicalPath), Seq(Row(Seq(1, 2))))

      // Read with isnotnull filter — the filter should not be pushed down into
      // the Parquet reader for repeated primitive fields (SPARK-39393), but the
      // query should still return correct results by evaluating the filter after
      // reading.
      checkAnswer(
        spark.read.parquet(dir.getCanonicalPath).filter("isnotnull(f)"),
        Seq(Row(Seq(1, 2))))
    }
  }

  test("issue #4136: struct with all requested fields missing in file") {
    // SPARK-53535 (Spark 4.1) added LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.
    // With the new default (false), Spark's vectorized reader appends a "marker" leaf field to
    // the Parquet read schema so it can distinguish a null parent row from a non-null parent
    // whose requested fields are all missing from the file, then truncates the marker out of
    // the ColumnarBatch. Comet's native scans don't implement this, so they conflate the two
    // cases and return Row(null) for non-null parents.
    assume(
      isSpark41Plus,
      "LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING was introduced in Spark 4.1")

    val tableSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_1", IntegerType)
        .add("_2", StringType),
      nullable = true)

    // Read schema requests _3, _4 — fields that don't exist in the file's _1 struct.
    val readSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_3", IntegerType, nullable = true)
        .add("_4", LongType, nullable = true),
      nullable = true)

    val data = java.util.Arrays.asList(
      Row(Row(1, "a")), // non-null parent, requested fields missing in file
      Row(Row(2, null)), // non-null parent, requested fields missing in file
      Row(null) // null parent
    )

    withTempPath { path =>
      spark
        .createDataFrame(data, tableSchema)
        .write
        .parquet(path.getCanonicalPath)

      // Mirror the toggles in Spark's `vectorized reader: missing all struct fields` test in
      // ParquetIOSuite, including off-heap on/off and the explicit nested-column vectorized
      // reader flag. We've seen CI fail on the off-heap branch when the on-heap branch passes.
      for {
        offheapEnabled <- Seq("true", "false")
        legacy <- Seq("true", "false")
      } withSQLConf(
        "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true",
        "spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing" -> legacy,
        "spark.sql.columnVector.offheap.enabled" -> offheapEnabled) {
        val df = spark.read.schema(readSchema).parquet(path.getCanonicalPath)
        checkSparkAnswer(df)
      }
    }
  }

  test("issue #4136: struct with only NullType fields in file (SPARK-54220)") {
    // The upstream SPARK-54220 test writes `Tuple1((null, null))` which is inferred as a struct
    // of NullType fields on disk; reading with a schema that asks for Int/String on top of
    // NullType fails at parquet decode time because Spark encodes NullType as
    // `BOOLEAN + LogicalType::Unknown` but parquet-rs only accepts `INT32 + Unknown`. See
    // #4199 for the upstream compatibility gap.
    assume(
      false,
      "Skipped until parquet-rs accepts BOOLEAN + Unknown for NullType " +
        "(https://github.com/apache/datafusion-comet/issues/4199)")

    val tableSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_1", NullType)
        .add("_2", NullType),
      nullable = true)

    val readSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_3", IntegerType, nullable = true)
        .add("_4", StringType, nullable = true),
      nullable = true)

    val data =
      java.util.Arrays.asList(Row(Row(null, null)), Row(Row(null, null)), Row(null))

    withTempPath { path =>
      spark
        .createDataFrame(data, tableSchema)
        .write
        .parquet(path.getCanonicalPath)

      for {
        offheapEnabled <- Seq("true", "false")
        legacy <- Seq("true", "false")
      } withSQLConf(
        "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true",
        "spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing" -> legacy,
        "spark.sql.columnVector.offheap.enabled" -> offheapEnabled) {
        val df = spark.read.schema(readSchema).parquet(path.getCanonicalPath)
        checkSparkAnswer(df)
      }
    }
  }

  /** Write a Parquet file using a raw RecordConsumer for full schema control. */
  private def writeDirect(
      path: String,
      schema: String,
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    val messageType = MessageTypeParser.parseMessageType(schema)
    val writeSupport = new DirectWriteSupport(messageType)
    class Builder extends ParquetWriter.Builder[RecordConsumer => Unit, Builder](new Path(path)) {
      override def getWriteSupport(conf: Configuration): WriteSupport[RecordConsumer => Unit] =
        writeSupport
      override def self(): Builder = this
    }
    val writer = new Builder().build()
    try recordWriters.foreach(writer.write)
    finally writer.close()
  }
}

private class DirectWriteSupport(schema: org.apache.parquet.schema.MessageType)
    extends WriteSupport[RecordConsumer => Unit] {
  private var recordConsumer: RecordConsumer = _

  override def init(configuration: Configuration): WriteContext =
    new WriteContext(schema, java.util.Collections.emptyMap())

  override def write(recordWriter: RecordConsumer => Unit): Unit =
    recordWriter(recordConsumer)

  override def prepareForWrite(rc: RecordConsumer): Unit =
    this.recordConsumer = rc
}
