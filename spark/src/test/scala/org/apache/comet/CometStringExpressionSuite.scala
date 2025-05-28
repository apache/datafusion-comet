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

import org.apache.spark.sql.CometTestBase

class CometStringExpressionSuite extends CometTestBase {

  test("Various String scalar functions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
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
    }
  }

  test("Upper and Lower") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
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
  }

  test("Chr") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col varchar(20)) using parquet")
          sql(
            s"insert into $table values('65'), ('66'), ('67'), ('68'), ('65'), ('66'), ('67'), ('68')")
          checkSparkAnswerAndOperator(s"SELECT chr(col) FROM $table")
        }
      }
    }
  }

  test("Chr with null character") {
    // test compatibility with Spark, spark supports chr(0)
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test0"
        withTable(table) {
          sql(s"create table $table(c9 int, c4 int) using parquet")
          sql(s"insert into $table values(0, 0), (66, null), (null, 70), (null, null)")
          val query = s"SELECT chr(c9), chr(c4) FROM $table"
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("Chr with negative and large value") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test0"
        withTable(table) {
          sql(s"create table $table(c9 int, c4 int) using parquet")
          sql(
            s"insert into $table values(0, 0), (61231, -61231), (-1700, 1700), (0, -4000), (-40, 40), (256, 512)")
          val query = s"SELECT chr(c9), chr(c4) FROM $table"
          checkSparkAnswerAndOperator(query)
        }
      }
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

  test("InitCap") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(id int, name varchar(20)) using parquet")
          sql(
            s"insert into $table values(1, 'james smith'), (2, 'michael rose'), " +
              "(3, 'robert williams'), (4, 'rames rose'), (5, 'james smith'), " +
              "(6, 'robert rose-smith'), (7, 'james ähtäri')")
          if (CometConf.COMET_EXEC_INITCAP_ENABLED.get()) {
            // TODO: remove this if clause https://github.com/apache/datafusion-comet/issues/1052
            checkSparkAnswerAndOperator(s"SELECT initcap(name) FROM $table")
          } else {
            checkSparkAnswer(s"SELECT initcap(name) FROM $table")
          }
        }
      }
    }
  }

  test("trim") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
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
  }

  test("string concat_ws") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
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
    }
  }

  test("string repeat") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(id int, name varchar(20)) using parquet")
          sql(s"insert into $table values(1, 'James'), (2, 'Smith'), (3, 'Smith')")
          checkSparkAnswerAndOperator(s"SELECT repeat(name, 3) FROM $table")
        }
      }
    }
  }

  test("length, reverse, instr, replace, translate") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col string) using parquet")
          sql(
            s"insert into $table values('Spark SQL  '), (NULL), (''), ('苹果手机'), ('Spark SQL  '), (NULL), (''), ('苹果手机')")
          checkSparkAnswerAndOperator("select length(col), reverse(col), instr(col, 'SQL'), instr(col, '手机'), replace(col, 'SQL', '123')," +
            s" replace(col, 'SQL'), replace(col, '手机', '平板'), translate(col, 'SL苹', '123') from $table")
        }
      }
    }
  }

}
