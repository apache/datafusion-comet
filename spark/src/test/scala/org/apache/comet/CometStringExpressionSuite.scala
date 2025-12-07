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

import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometStringExpressionSuite extends CometTestBase {

  test("lpad string") {
    testStringPadding("lpad")
  }

  test("rpad string") {
    testStringPadding("rpad")
  }

  test("lpad binary") {
    testBinaryPadding("lpad")
  }

  test("rpad binary") {
    testBinaryPadding("rpad")
  }

  private def testStringPadding(expr: String): Unit = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("str", DataTypes.StringType, nullable = true),
        StructField("len", DataTypes.IntegerType, nullable = true),
        StructField("pad", DataTypes.StringType, nullable = true)))
    // scalastyle:off
    val edgeCases = Seq(
      "é", // unicode 'e\\u{301}'
      "é", // unicode '\\u{e9}'
      "తెలుగు")
    // scalastyle:on
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(maxStringLength = 6, customStrings = edgeCases))
    df.createOrReplaceTempView("t1")

    // test all combinations of scalar and array arguments
    for (str <- Seq("'hello'", "str")) {
      for (len <- Seq("6", "-6", "0", "len % 10")) {
        for (pad <- Seq(Some("'x'"), Some("'zzz'"), Some("pad"), None)) {
          val sql = pad match {
            case Some(p) =>
              // 3 args
              s"SELECT $str, $len, $expr($str, $len, $p) FROM t1 ORDER BY str, len, pad"
            case _ =>
              // 2 args (default pad of ' ')
              s"SELECT $str, $len, $expr($str, $len) FROM t1 ORDER BY str, len, pad"
          }
          val isLiteralStr = str == "'hello'"
          val isLiteralLen = !len.contains("len")
          val isLiteralPad = !pad.contains("pad")
          if (isLiteralStr && isLiteralLen && isLiteralPad) {
            // all arguments are literal, so Spark constant folding will kick in
            // and pad function will not be evaluated by Comet
            checkSparkAnswerAndOperator(sql)
          } else if (isLiteralStr) {
            checkSparkAnswerAndFallbackReason(
              sql,
              "Scalar values are not supported for the str argument")
          } else if (!isLiteralPad) {
            checkSparkAnswerAndFallbackReason(
              sql,
              "Only scalar values are supported for the pad argument")
          } else {
            checkSparkAnswerAndOperator(sql)
          }
        }
      }
    }
  }

  private def testBinaryPadding(expr: String): Unit = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("str", DataTypes.BinaryType, nullable = true),
        StructField("len", DataTypes.IntegerType, nullable = true),
        StructField("pad", DataTypes.BinaryType, nullable = true)))
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
    df.createOrReplaceTempView("t1")

    // test all combinations of scalar and array arguments
    for (str <- Seq("unhex('DDEEFF')", "str")) {
      // Spark does not support negative length for lpad/rpad with binary input and Comet does
      // not support abs yet, so use `10 + len % 10` to avoid negative length
      for (len <- Seq("6", "0", "10 + len % 10")) {
        for (pad <- Seq(Some("unhex('CAFE')"), Some("pad"), None)) {

          val sql = pad match {
            case Some(p) =>
              // 3 args
              s"SELECT $str, $len, $expr($str, $len, $p) FROM t1 ORDER BY str, len, pad"
            case _ =>
              // 2 args (default pad of ' ')
              s"SELECT $str, $len, $expr($str, $len) FROM t1 ORDER BY str, len, pad"
          }

          val isLiteralStr = str != "str"
          val isLiteralLen = !len.contains("len")
          val isLiteralPad = !pad.contains("pad")

          if (isLiteralStr && isLiteralLen && isLiteralPad) {
            // all arguments are literal, so Spark constant folding will kick in
            // and pad function will not be evaluated by Comet
            checkSparkAnswerAndOperator(sql)
          } else {
            // Comet will fall back to Spark because the plan contains a staticinvoke instruction
            // which is not supported
            checkSparkAnswerAndFallbackReason(
              sql,
              s"Static invoke expression: $expr is not supported")
          }
        }
      }
    }
  }

  test("Various String scalar functions") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(
        s"insert into $table values(1, 'James Smith'), (2, 'Michael Rose')," +
          " (3, 'Robert Williams'), (4, 'Rames Rose'), (5, 'James Smith')")
      checkSparkAnswerAndOperator(
        s"SELECT ascii(name), bit_length(name), octet_length(name) FROM $table")
    }
  }

  test("Upper and Lower") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      val table = "names"
      withTable(table) {
        sql(s"create table $table(id int, name varchar(20)) using parquet")
        sql(
          s"insert into $table values(1, 'James Smith'), (2, 'Michael Rose')," +
            " (3, 'Robert Williams'), (4, 'Rames Rose'), (5, 'James Smith')")
        checkSparkAnswerAndOperator(s"SELECT name, upper(name), lower(name) FROM $table")
      }
    }
  }

  test("Chr") {
    val table = "test"
    withTable(table) {
      sql(s"create table $table(col varchar(20)) using parquet")
      sql(
        s"insert into $table values('65'), ('66'), ('67'), ('68'), ('65'), ('66'), ('67'), ('68')")
      checkSparkAnswerAndOperator(s"SELECT chr(col) FROM $table")
    }
  }

  test("Chr with null character") {
    // test compatibility with Spark, spark supports chr(0)
    val table = "test0"
    withTable(table) {
      sql(s"create table $table(c9 int, c4 int) using parquet")
      sql(s"insert into $table values(0, 0), (66, null), (null, 70), (null, null)")
      val query = s"SELECT chr(c9), chr(c4) FROM $table"
      checkSparkAnswerAndOperator(query)
    }
  }

  test("Chr with negative and large value") {
    val table = "test0"
    withTable(table) {
      sql(s"create table $table(c9 int, c4 int) using parquet")
      sql(
        s"insert into $table values(0, 0), (61231, -61231), (-1700, 1700), (0, -4000), (-40, 40), (256, 512)")
      val query = s"SELECT chr(c9), chr(c4) FROM $table"
      checkSparkAnswerAndOperator(query)
    }

    withParquetTable((0 until 5).map(i => (i % 5, i % 3)), "tbl") {
      withSQLConf(
        "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
        for (n <- Seq("0", "-0", "0.5", "-0.5", "555", "-555", "null")) {
          checkSparkAnswerAndOperator(s"select chr(cast(${n} as int)) FROM tbl")
        }
      }
    }
  }

  test("InitCap compatible cases") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      withSQLConf(CometConf.getExprAllowIncompatConfigKey("InitCap") -> "true") {
        sql(
          s"insert into $table values(1, 'james smith'), (2, 'michael rose'), " +
            "(3, 'robert williams'), (4, 'rames rose'), (5, 'james smith'), " +
            "(7, 'james ähtäri')")
        checkSparkAnswerAndOperator(s"SELECT initcap(name) FROM $table")
      }
    }
  }

  test("InitCap incompatible cases") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      // Comet and Spark differ on hyphenated names
      sql(s"insert into $table values(6, 'robert rose-smith')")
      checkSparkAnswer(s"SELECT initcap(name) FROM $table")
    }
  }

  test("trim") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      val table = "test"
      withTable(table) {
        sql(s"create table $table(col varchar(20)) using parquet")
        sql(s"insert into $table values('    SparkSQL   '), ('SSparkSQLS')")

        checkSparkAnswerAndOperator(s"SELECT upper(trim(col)) FROM $table")
        checkSparkAnswerAndOperator(s"SELECT trim('SL', col) FROM $table")

        checkSparkAnswerAndOperator(s"SELECT upper(btrim(col)) FROM $table")
        checkSparkAnswerAndOperator(s"SELECT btrim('SL', col) FROM $table")

        checkSparkAnswerAndOperator(s"SELECT upper(ltrim(col)) FROM $table")
        checkSparkAnswerAndOperator(s"SELECT ltrim('SL', col) FROM $table")

        checkSparkAnswerAndOperator(s"SELECT upper(rtrim(col)) FROM $table")
        checkSparkAnswerAndOperator(s"SELECT rtrim('SL', col) FROM $table")
      }
    }
  }

  test("string concat_ws") {
    val table = "names"
    withTable(table) {
      sql(
        s"create table $table(id int, first_name varchar(20), middle_initial char(1), last_name varchar(20)) using parquet")
      sql(
        s"insert into $table values(1, 'James', 'B', 'Taylor'), (2, 'Smith', 'C', 'Davis')," +
          " (3, NULL, NULL, NULL), (4, 'Smith', 'C', 'Davis')")
      checkSparkAnswerAndOperator(
        s"SELECT concat_ws(' ', first_name, middle_initial, last_name) FROM $table")
    }
  }

  test("string repeat") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1, 'James'), (2, 'Smith'), (3, 'Smith')")
      checkSparkAnswerAndOperator(s"SELECT repeat(name, 3) FROM $table")
    }
  }

  test("length, reverse, instr, replace, translate") {
    val table = "test"
    withTable(table) {
      sql(s"create table $table(col string) using parquet")
      sql(
        s"insert into $table values('Spark SQL  '), (NULL), (''), ('苹果手机'), ('Spark SQL  '), (NULL), (''), ('苹果手机')")
      checkSparkAnswerAndOperator("select length(col), reverse(col), instr(col, 'SQL'), instr(col, '手机'), replace(col, 'SQL', '123')," +
        s" replace(col, 'SQL'), replace(col, '手机', '平板'), translate(col, 'SL苹', '123') from $table")
    }
  }

  // Simplified version of "filter pushdown - StringPredicate" that does not generate dictionaries
  test("string predicate filter") {
    Seq(false, true).foreach { pushdown =>
      withSQLConf(
        SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED.key -> pushdown.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(name varchar(20)) using parquet")
          for (ch <- Range('a', 'z')) {
            sql(s"insert into $table values('$ch$ch$ch')")
          }
          checkSparkAnswerAndOperator(s"SELECT * FROM $table WHERE name LIKE 'a%'")
          checkSparkAnswerAndOperator(s"SELECT * FROM $table WHERE name LIKE '%a'")
          checkSparkAnswerAndOperator(s"SELECT * FROM $table WHERE name LIKE '%a%'")
        }
      }
    }
  }

  // Modified from Spark test "filter pushdown - StringPredicate"
  private def testStringPredicate(
      dataFrame: DataFrame,
      filter: String,
      enableDictionary: Boolean = true): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      dataFrame.write
        .option("parquet.block.size", 512)
        .option(ParquetOutputFormat.ENABLE_DICTIONARY, enableDictionary)
        .parquet(path)
      Seq(true, false).foreach { pushDown =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED.key -> pushDown.toString) {
          val df = spark.read.parquet(path).filter(filter)
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  // Modified from Spark test "filter pushdown - StringPredicate"
  test("filter pushdown - StringPredicate") {
    import testImplicits._
    // keep() should take effect on StartsWith/EndsWith/Contains
    Seq(
      "value like 'a%'", // StartsWith
      "value like '%a'", // EndsWith
      "value like '%a%'" // Contains
    ).foreach { filter =>
      testStringPredicate(
        // dictionary will be generated since there are duplicated values
        spark.range(1000).map(t => (t % 10).toString).toDF(),
        filter)
    }

    // canDrop() should take effect on StartsWith,
    // and has no effect on EndsWith/Contains
    Seq(
      "value like 'a%'", // StartsWith
      "value like '%a'", // EndsWith
      "value like '%a%'" // Contains
    ).foreach { filter =>
      testStringPredicate(
        spark.range(1024).map(_.toString).toDF(),
        filter,
        enableDictionary = false)
    }

    // inverseCanDrop() should take effect on StartsWith,
    // and has no effect on EndsWith/Contains
    Seq(
      "value not like '10%'", // StartsWith
      "value not like '%10'", // EndsWith
      "value not like '%10%'" // Contains
    ).foreach { filter =>
      testStringPredicate(
        spark.range(1024).map(_ => "100").toDF(),
        filter,
        enableDictionary = false)
    }
  }

  test("string_space") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      checkSparkAnswerAndOperator("SELECT space(_1), space(_2) FROM tbl")
    }
  }

  test("string_space with dictionary") {
    val data = (0 until 1000).map(i => Tuple1(i % 5))

    withSQLConf("parquet.enable.dictionary" -> "true") {
      withParquetTable(data, "tbl") {
        checkSparkAnswerAndOperator("SELECT space(_1) FROM tbl")
      }
    }
  }

  test("regexp_extract basic") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("100-200", 1),
        ("300-400", 1),
        (null, 1), // NULL input
        ("no-match", 1), // no match → should return ""
        ("abc123def456", 1),
        ("", 1) // empty string
      )

      withParquetTable(data, "tbl") {
        // Test basic extraction: group 0 (full match)
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)-(\\d+)', 0) FROM tbl")
        // Test group 1
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)-(\\d+)', 1) FROM tbl")
        // Test group 2
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)-(\\d+)', 2) FROM tbl")
        // Test non-existent group → should return ""
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)-(\\d+)', 3) FROM tbl")
        // Test empty pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '', 0) FROM tbl")
        // Test null pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, NULL, 0) FROM tbl")
      }
    }
  }

  test("regexp_extract edge cases") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data =
        Seq(("email@example.com", 1), ("phone: 123-456-7890", 1), ("price: $99.99", 1), (null, 1))

      withParquetTable(data, "tbl") {
        // Extract email domain
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '@([^.]+)', 1) FROM tbl")
        // Extract phone number
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d{3}-\\d{3}-\\d{4})', 1) FROM tbl")
        // Extract price
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '\\$(\\d+\\.\\d+)', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract_all basic") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("a1b2c3", 1),
        ("test123test456", 1),
        (null, 1), // NULL input
        ("no digits", 1), // no match → should return []
        ("", 1) // empty string
      )

      withParquetTable(data, "tbl") {
        // Test with explicit group 0 (full match on no-group pattern)
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '\\d+', 0) FROM tbl")
        // Test with explicit group 0
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 0) FROM tbl")
        // Test group 1
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 1) FROM tbl")
        // Test empty pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '', 0) FROM tbl")
        // Test null pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, NULL, 0) FROM tbl")
      }
    }
  }

  test("regexp_extract_all multiple matches") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("The prices are $10, $20, and $30", 1),
        ("colors: red, green, blue", 1),
        ("words: hello world", 1),
        (null, 1))

      withParquetTable(data, "tbl") {
        // Extract all prices
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '\\$(\\d+)', 1) FROM tbl")
        // Extract all words
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-z]+)', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract_all with dictionary encoding") {
    withSQLConf(
      CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true",
      "parquet.enable.dictionary" -> "true") {
      // Use repeated values to trigger dictionary encoding
      // Mix short strings, long strings, and various patterns
      val longString1 = "prefix" + ("abc" * 100) + "123" + ("xyz" * 100) + "456"
      val longString2 = "start" + ("test" * 200) + "789" + ("end" * 150)

      val data = (0 until 2000).map(i => {
        val text = i % 7 match {
          case 0 => "a1b2c3" // Simple repeated pattern
          case 1 => "x5y6" // Another simple pattern
          case 2 => "no-match" // No digits
          case 3 => longString1 // Long string with digits
          case 4 => longString2 // Another long string
          case 5 => "email@test.com-phone:123-456-7890" // Complex pattern
          case 6 => "" // Empty string
        }
        (text, 1)
      })

      withParquetTable(data, "tbl") {
        // Test simple pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)') FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 0) FROM tbl")

        // Test complex patterns
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '(\\d{3}-\\d{3}-\\d{4})', 0) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '@([a-z]*)', 1) FROM tbl")

        // Test with multiple groups
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-z])(\\d*)', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract with dictionary encoding") {
    withSQLConf(
      CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true",
      "parquet.enable.dictionary" -> "true") {
      // Use repeated values to trigger dictionary encoding
      // Mix short and long strings with various patterns
      val longString1 = "data" + ("x" * 500) + "999" + ("y" * 500)
      val longString2 = ("a" * 1000) + "777" + ("b" * 1000)

      val data = (0 until 2000).map(i => {
        val text = i % 7 match {
          case 0 => "a1b2c3"
          case 1 => "x5y6"
          case 2 => "no-match"
          case 3 => longString1
          case 4 => longString2
          case 5 => "IP:192.168.1.100-PORT:8080"
          case 6 => ""
        }
        (text, 1)
      })

      withParquetTable(data, "tbl") {
        // Test extracting first match with simple pattern
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)', 1) FROM tbl")

        // Test with complex patterns
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, 'PORT:(\\d+)', 1) FROM tbl")

        // Test with multiple groups - extract second group
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '([a-z])(\\d+)', 2) FROM tbl")
      }
    }
  }

  test("regexp_extract unicode and special characters") {
    import org.apache.comet.CometConf

    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("测试123test", 1), // Chinese characters
        ("日本語456にほんご", 1), // Japanese characters
        ("한글789Korean", 1), // Korean characters
        ("Привет999Hello", 1), // Cyrillic
        ("line1\nline2", 1), // Newline
        ("tab\there", 1), // Tab
        ("special: $#@!%^&*", 1), // Special chars
        ("mixed测试123test日本語", 1), // Mixed unicode
        (null, 1))

      withParquetTable(data, "tbl") {
        // Extract digits from unicode text
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\d+)', 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 1) FROM tbl")

        // Test word boundaries with unicode
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '([a-zA-Z]+)', 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-zA-Z]+)', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract_all multiple groups") {
    import org.apache.comet.CometConf

    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("a1b2c3", 1),
        ("x5y6z7", 1),
        ("test123demo456end789", 1),
        (null, 1),
        ("no match here", 1))

      withParquetTable(data, "tbl") {
        // Test extracting different groups - full match
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-z])(\\d+)', 0) FROM tbl")
        // Test extracting group 1 (letters)
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-z])(\\d+)', 1) FROM tbl")
        // Test extracting group 2 (digits)
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '([a-z])(\\d+)', 2) FROM tbl")

        // Test with three groups
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\d+)([a-z]+)', 1) FROM tbl")
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\d+)([a-z]+)', 2) FROM tbl")
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\d+)([a-z]+)', 3) FROM tbl")
      }
    }
  }

  test("regexp_extract_all group index out of bounds") {
    import org.apache.comet.CometConf

    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(("a1b2c3", 1), ("test123", 1), (null, 1))

      withParquetTable(data, "tbl") {
        // Group index out of bounds - should match Spark's behavior (error or empty)
        // Pattern has only 1 group, asking for group 2
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 2) FROM tbl")

        // Pattern has no groups, asking for group 1
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '\\d+', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract complex patterns") {
    import org.apache.comet.CometConf

    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(
        ("2024-01-15", 1), // Date
        ("192.168.1.1", 1), // IP address
        ("user@domain.co.uk", 1), // Complex email
        ("<tag>content</tag>", 1), // HTML-like
        ("Time: 14:30:45.123", 1), // Timestamp
        ("Version: 1.2.3-beta", 1), // Version string
        ("RGB(255,128,0)", 1), // RGB color
        (null, 1))

      withParquetTable(data, "tbl") {
        // Extract year from date
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d{4})-(\\d{2})-(\\d{2})', 1) FROM tbl")

        // Extract month from date
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d{4})-(\\d{2})-(\\d{2})', 2) FROM tbl")

        // Extract IP octets
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 2) FROM tbl")

        // Extract email domain
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '@([a-z.]+)', 1) FROM tbl")

        // Extract time components
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\d{2}):(\\d{2}):(\\d{2})', 1) FROM tbl")

        // Extract RGB values
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, 'RGB\\((\\d+),(\\d+),(\\d+)\\)', 2) FROM tbl")

        // Test regexp_extract_all with complex patterns
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\d+)', 1) FROM tbl")
      }
    }
  }

  test("regexp_extract vs regexp_extract_all comparison") {
    import org.apache.comet.CometConf

    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq(("a1b2c3", 1), ("x5y6", 1), (null, 1), ("no digits", 1), ("single7match", 1))

      withParquetTable(data, "tbl") {
        // Compare single extraction vs all extractions in one query
        checkSparkAnswerAndOperator("""SELECT 
            |  regexp_extract(_1, '(\\d+)', 1) as first_match,
            |  regexp_extract_all(_1, '(\\d+)', 1) as all_matches
            |FROM tbl""".stripMargin)

        // Verify regexp_extract returns first match only while regexp_extract_all returns all
        checkSparkAnswerAndOperator("""SELECT 
            |  _1,
            |  regexp_extract(_1, '(\\d+)', 1) as first_digit,
            |  regexp_extract_all(_1, '(\\d+)', 1) as all_digits
            |FROM tbl""".stripMargin)

        // Test with multiple groups
        checkSparkAnswerAndOperator("""SELECT
            |  regexp_extract(_1, '([a-z])(\\d+)', 1) as first_letter,
            |  regexp_extract_all(_1, '([a-z])(\\d+)', 1) as all_letters,
            |  regexp_extract(_1, '([a-z])(\\d+)', 2) as first_digit,
            |  regexp_extract_all(_1, '([a-z])(\\d+)', 2) as all_digits
            |FROM tbl""".stripMargin)
      }
    }
  }

}
