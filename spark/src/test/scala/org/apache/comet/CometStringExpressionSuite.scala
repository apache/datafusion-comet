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
  // scalastyle:off
  private val edgeCases = Seq(
    "é", // unicode 'e\\u{301}'
    "é", // unicode '\\u{e9}'
    "తెలుగు")
  // scalastyle:on

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

  test("split string basic") {
    withSQLConf("spark.comet.expression.StringSplit.allowIncompatible" -> "true") {
      withParquetTable((0 until 5).map(i => (s"value$i,test$i", i)), "tbl") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl")
        checkSparkAnswerAndOperator("SELECT split('one,two,three', ',') FROM tbl")
        checkSparkAnswerAndOperator("SELECT split(_1, '-') FROM tbl")
      }
    }
  }

  test("split string with limit") {
    withSQLConf("spark.comet.expression.StringSplit.allowIncompatible" -> "true") {
      withParquetTable((0 until 5).map(i => ("a,b,c,d,e", i)), "tbl") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',', 2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT split(_1, ',', 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT split(_1, ',', -1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT split(_1, ',', 0) FROM tbl")
      }
    }
  }

  test("split string with regex patterns") {
    withSQLConf("spark.comet.expression.StringSplit.allowIncompatible" -> "true") {
      withParquetTable((0 until 5).map(i => ("word1 word2  word3", i)), "tbl") {
        checkSparkAnswerAndOperator("SELECT split(_1, ' ') FROM tbl")
        checkSparkAnswerAndOperator("SELECT split(_1, '\\\\s+') FROM tbl")
      }

      withParquetTable((0 until 5).map(i => ("foo123bar456baz", i)), "tbl2") {
        checkSparkAnswerAndOperator("SELECT split(_1, '\\\\d+') FROM tbl2")
      }
    }
  }

  test("split string edge cases") {
    withSQLConf("spark.comet.expression.StringSplit.allowIncompatible" -> "true") {
      withParquetTable(Seq(("", 0), ("single", 1), (null, 2), ("a", 3)), "tbl") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl")
      }
    }
  }

  test("split string with UTF-8 characters") {
    withSQLConf("spark.comet.expression.StringSplit.allowIncompatible" -> "true") {
      // CJK characters
      withParquetTable(Seq(("你好,世界", 0), ("こんにちは,世界", 1)), "tbl_cjk") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl_cjk")
      }

      // Emoji and symbols
      withParquetTable(Seq(("😀,😃,😄", 0), ("🔥,💧,🌍", 1), ("α,β,γ", 2)), "tbl_emoji") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl_emoji")
      }

      // Combining characters / grapheme clusters
      withParquetTable(
        Seq(
          ("café,naïve", 0), // precomposed
          ("café,naïve", 1), // combining (if your editor supports it)
          ("मानक,हिन्दी", 2)
        ), // Devanagari script
        "tbl_graphemes") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl_graphemes")
      }

      // Mixed ASCII and multi-byte with regex patterns
      withParquetTable(
        Seq(("hello世界test你好", 0), ("foo😀bar😃baz", 1), ("abc한글def", 2)), // Korean Hangul
        "tbl_mixed") {
        // Split on ASCII word boundaries
        checkSparkAnswerAndOperator("SELECT split(_1, '[a-z]+') FROM tbl_mixed")
      }

      // RTL (Right-to-Left) characters
      withParquetTable(Seq(("مرحبا,عالم", 0), ("שלום,עולם", 1)), "tbl_rtl") { // Arabic, Hebrew
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl_rtl")
      }

      // Zero-width characters and special Unicode
      withParquetTable(
        Seq(
          ("test\u200Bword", 0), // Zero-width space
          ("foo\u00ADbar", 1)
        ), // Soft hyphen
        "tbl_special") {
        checkSparkAnswerAndOperator("SELECT split(_1, '\u200B') FROM tbl_special")
      }

      // Surrogate pairs (4-byte UTF-8)
      withParquetTable(
        Seq(
          ("𝐇𝐞𝐥𝐥𝐨,𝐖𝐨𝐫𝐥𝐝", 0), // Mathematical bold letters (U+1D400 range)
          ("𠜎,𠜱,𠝹", 1)
        ), // CJK Extension B
        "tbl_surrogate") {
        checkSparkAnswerAndOperator("SELECT split(_1, ',') FROM tbl_surrogate")
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
    withParquetTable((0 until 5).map(i => (-i, i + 1)), "tbl") {
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

  test("substring") {
    val data = Seq(("hello world", ""), ("", ""), (null, ""), ("abc", ""))
    withParquetTable(data, "tbl") {
      // positive start
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 5) FROM tbl")
      // negative start, no length
      checkSparkAnswerAndOperator("SELECT substring(_1, -3) FROM tbl")
      // zero start
      checkSparkAnswerAndOperator("SELECT substring(_1, 0, 3) FROM tbl")
      // zero length
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 0) FROM tbl")
      // negative length
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, -1) FROM tbl")
      // start beyond string length
      checkSparkAnswerAndOperator("SELECT substring(_1, 100) FROM tbl")
      // negative start with length
      checkSparkAnswerAndOperator("SELECT substring(_1, -2, 3) FROM tbl")
      // negative start beyond string length with length
      checkSparkAnswerAndOperator("SELECT substring(_1, -10, 3) FROM tbl")
      // large negative start with length
      checkSparkAnswerAndOperator("SELECT substring(_1, -300, 3) FROM tbl")
    }
  }

  test("substring - negative start boundary cases") {
    // "abc" has length 3, so -3 means start at first char, -4 exceeds length
    val data = Seq(("abc", ""), ("a", ""), ("ab", ""), ("", ""), (null, ""))
    withParquetTable(data, "tbl") {
      // abs(start) == string length exactly (boundary: should return from first char)
      checkSparkAnswerAndOperator("SELECT substring(_1, -3, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -3) FROM tbl")
      // abs(start) == length + 1 (one past boundary: should return empty)
      checkSparkAnswerAndOperator("SELECT substring(_1, -4, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -4) FROM tbl")
      // abs(start) == length - 1 (one before boundary)
      checkSparkAnswerAndOperator("SELECT substring(_1, -2, 5) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -2) FROM tbl")
      // -1: last character
      checkSparkAnswerAndOperator("SELECT substring(_1, -1, 1) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -1) FROM tbl")
      // -1 with length exceeding remaining chars
      checkSparkAnswerAndOperator("SELECT substring(_1, -1, 100) FROM tbl")
    }
  }

  test("substring - negative start with zero and negative length") {
    val data = Seq(("hello", ""), ("ab", ""), ("", ""), (null, ""))
    withParquetTable(data, "tbl") {
      // negative start + zero length
      checkSparkAnswerAndOperator("SELECT substring(_1, -3, 0) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -100, 0) FROM tbl")
      // negative start + negative length
      checkSparkAnswerAndOperator("SELECT substring(_1, -3, -1) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -1, -5) FROM tbl")
      // negative start exceeding length + zero length
      checkSparkAnswerAndOperator("SELECT substring(_1, -10, 0) FROM tbl")
      // negative start exceeding length + negative length
      checkSparkAnswerAndOperator("SELECT substring(_1, -10, -1) FROM tbl")
    }
  }

  test("substring - single character and empty strings") {
    val data = Seq(("x", ""), ("", ""), (null, ""))
    withParquetTable(data, "tbl") {
      for (start <- Seq(-2, -1, 0, 1, 2)) {
        for (len <- Seq(0, 1, 5)) {
          checkSparkAnswerAndOperator(s"SELECT substring(_1, $start, $len) FROM tbl")
        }
        // without explicit length
        checkSparkAnswerAndOperator(s"SELECT substring(_1, $start) FROM tbl")
      }
    }
  }

  test("substring - unicode multi-byte characters") {
    // scalastyle:off
    val data = Seq(
      ("苹果手机", ""), // 4 Chinese characters (3 bytes each in UTF-8)
      ("café", ""), // combining accent
      ("😀🎉🔥", ""), // emoji (4 bytes each in UTF-8)
      ("aé苹😀", ""), // mixed: ASCII + 2-byte + 3-byte + 4-byte
      ("", ""),
      (null, ""))
    // scalastyle:on
    withParquetTable(data, "tbl") {
      // positive start into multi-byte
      checkSparkAnswerAndOperator("SELECT substring(_1, 2, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 1) FROM tbl")
      // negative start with multi-byte
      checkSparkAnswerAndOperator("SELECT substring(_1, -2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -2, 1) FROM tbl")
      // negative start exceeding multi-byte string length
      checkSparkAnswerAndOperator("SELECT substring(_1, -10, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -10) FROM tbl")
      // abs(start) == char length boundary for 4-char string
      checkSparkAnswerAndOperator("SELECT substring(_1, -4, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -5, 2) FROM tbl")
      // extract entire string
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 100) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, 1) FROM tbl")
    }
  }

  test("substring - decomposed and combining unicode characters") {
    val data = edgeCases.map(s => (s, "")) :+ ("", "") :+ (null, "")
    withParquetTable(data, "tbl") {
      // first code point only — exposes decomposed vs precomposed difference
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 1) FROM tbl")
      // second code point — combining accent for decomposed, nothing for precomposed
      checkSparkAnswerAndOperator("SELECT substring(_1, 2, 1) FROM tbl")
      // full string
      checkSparkAnswerAndOperator("SELECT substring(_1, 1) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 100) FROM tbl")
      // negative start — last code point
      checkSparkAnswerAndOperator("SELECT substring(_1, -1, 1) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -1) FROM tbl")
      // negative start — last 2 code points
      checkSparkAnswerAndOperator("SELECT substring(_1, -2, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -2) FROM tbl")
      // middle of Telugu string
      checkSparkAnswerAndOperator("SELECT substring(_1, 3, 2) FROM tbl")
      // start beyond string length
      checkSparkAnswerAndOperator("SELECT substring(_1, 10) FROM tbl")
      // negative start beyond string length
      checkSparkAnswerAndOperator("SELECT substring(_1, -10, 3) FROM tbl")
      checkSparkAnswerAndOperator("SELECT substring(_1, -10) FROM tbl")
      // zero length
      checkSparkAnswerAndOperator("SELECT substring(_1, 1, 0) FROM tbl")
      // negative length
      checkSparkAnswerAndOperator("SELECT substring(_1, -2, -1) FROM tbl")
    }
  }

  test("substring - large start and length values") {
    val data = Seq(("hello world", ""), ("abc", ""), ("", ""), (null, ""))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator(s"SELECT substring(_1, ${Int.MaxValue}, 5) FROM tbl")
      checkSparkAnswerAndOperator(s"SELECT substring(_1, 1, ${Int.MaxValue}) FROM tbl")
      checkSparkAnswerAndOperator(s"SELECT substring(_1, ${Int.MinValue + 1}, 5) FROM tbl")
      checkSparkAnswerAndOperator(s"SELECT substring(_1, ${Int.MinValue + 1}) FROM tbl")
      checkSparkAnswerAndOperator(
        s"SELECT substring(_1, ${Int.MaxValue}, ${Int.MaxValue}) FROM tbl")
    }
  }

  test("substring - dictionary encoded strings") {
    // repeated values to trigger dictionary encoding
    val data = (0 until 1000).map { i =>
      val s = i % 5 match {
        case 0 => "hello"
        case 1 => "ab"
        case 2 => ""
        case 3 => null
        case 4 => "world!"
      }
      Tuple1(s)
    }
    withSQLConf("parquet.enable.dictionary" -> "true") {
      withParquetTable(data, "tbl") {
        // positive start
        checkSparkAnswerAndOperator("SELECT substring(_1, 2, 3) FROM tbl")
        // negative start within bounds
        checkSparkAnswerAndOperator("SELECT substring(_1, -3, 2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring(_1, -3) FROM tbl")
        // negative start exceeding length for some values
        checkSparkAnswerAndOperator("SELECT substring(_1, -4, 2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring(_1, -4) FROM tbl")
        // negative start exceeding all string lengths
        checkSparkAnswerAndOperator("SELECT substring(_1, -100, 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring(_1, -100) FROM tbl")
        // zero start
        checkSparkAnswerAndOperator("SELECT substring(_1, 0, 3) FROM tbl")
        // -1 last char
        checkSparkAnswerAndOperator("SELECT substring(_1, -1, 1) FROM tbl")
      }
    }
  }

  test("substring - scalar inputs") {
    val noConstantFolding =
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val data = Seq(("hello world", ""), ("abc", ""), ("", ""), (null, ""))
    withSQLConf(noConstantFolding) {
      withParquetTable(data, "tbl") {
        // all-literal arguments
        checkSparkAnswerAndOperator("SELECT substring('hello world', 1, 5) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', -3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', 0, 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', 1, 0) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', 1, -1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', 100) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('', 1, 5) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring(NULL, 1, 5) FROM tbl")
        // negative start edge cases
        checkSparkAnswerAndOperator("SELECT substring('hello world', -2, 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', -10, 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('hello world', -300, 3) FROM tbl")
        // scalar alongside column
        checkSparkAnswerAndOperator(
          "SELECT substring(_1, 1, 5), substring('hello', 1, 5) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring(_1, -3), substring('world', -3) FROM tbl")
      }
    }
  }

  test("substring - scalar inputs with multi-byte") {
    val noConstantFolding =
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    // scalastyle:off
    val data = Seq(Tuple1("placeholder"))
    withSQLConf(noConstantFolding) {
      withParquetTable(data, "tbl") {
        checkSparkAnswerAndOperator("SELECT substring('こんにちは世界', 1, 3) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('こんにちは世界', -2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('🎉🎊🎈🎁', 2, 2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('ab🎉cd', 3, 1) FROM tbl")
        // decomposed vs precomposed
        checkSparkAnswerAndOperator("SELECT substring('é', 1, 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('é', 1, 1) FROM tbl")
        // Telugu
        checkSparkAnswerAndOperator("SELECT substring('తెలుగు', 1, 2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT substring('తెలుగు', -2, 2) FROM tbl")
      }
    }
    // scalastyle:on
  }

  test("levenshtein") {
    val data =
      Seq(("kitten", "sitting"), ("frog", "fog"), ("abc", "abc"), ("", "hello"), ("hello", ""))

    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT levenshtein(_1, _2) FROM tbl")
    }
  }

  test("levenshtein with nulls") {
    val table = "levenshtein_null_test"
    withTable(table) {
      sql(s"CREATE TABLE $table(s1 STRING, s2 STRING) USING parquet")
      sql(
        s"INSERT INTO $table VALUES " +
          "('abc', 'adc'), (NULL, 'test'), ('hello', NULL), (NULL, NULL)")
      checkSparkAnswerAndOperator(s"SELECT levenshtein(s1, s2) FROM $table")
    }
  }

  test("levenshtein with unicode") {
    val data = Seq(
      ("\u4f60\u597d", "\u4f60\u574f"),
      ("caf\u00e9", "cafe"),
      ("\ud83d\ude00", "\ud83d\ude01"))

    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT levenshtein(_1, _2) FROM tbl")
    }
  }

  test("levenshtein with threshold") {
    val data = Seq(("kitten", "sitting"), ("frog", "fog"), ("abc", "abc"), ("hello", "world"))

    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT levenshtein(_1, _2, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT levenshtein(_1, _2, 0) FROM tbl")
      checkSparkAnswerAndOperator("SELECT levenshtein(_1, _2, 10) FROM tbl")
    }
  }

}
