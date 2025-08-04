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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataTypes, StringType}

import org.apache.comet.serde._

class CometStringExpressionSerdeSuite extends CometTestBase {

  test("CometStartsWith") {
    val startsWith = StartsWith(Literal("Apache Spark"), Literal("Apache"))
    val proto = CometStartsWith.convert(startsWith, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "starts_with"
                     |  args {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometEndsWith") {
    val endsWith = EndsWith(Literal("Apache Spark"), Literal("Spark"))
    val proto = CometEndsWith.convert(endsWith, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "ends_with"
                     |  args {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometContains") {
    val contains = Contains(Literal("Apache Spark"), Literal("che"))
    val proto = CometContains.convert(contains, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "contains"
                     |  args {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "che"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringSpace") {
    val stringSpace = StringSpace(Literal(5))
    val proto = CometStringSpace.convert(stringSpace, Seq.empty, binding = true)
    val expected = """string_space {
                     |  child {
                     |    literal {
                     |      int_val: 5
                     |      datatype {
                     |        type_id: INT32
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometSubstring with literal positions") {
    val substring = Substring(Literal("Apache Spark"), Literal(1), Literal(6))
    val proto = CometSubstring.convert(substring, Seq.empty, binding = true)
    val expected = """substring {
                     |  child {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  start: 1
                     |  len: 6
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometSubstring with non-literal position returns None") {
    val substring = Substring(
      Literal("Apache Spark"),
      AttributeReference("pos", DataTypes.IntegerType, true)(),
      Literal(6))
    val proto = CometSubstring.convert(substring, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometLike with valid escape character") {
    val like = Like(Literal("Apache Spark"), Literal("Apache%"), '\\')
    val proto = CometLike.convert(like, Seq.empty, binding = true)
    val expected = """like {
                     |  left {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  right {
                     |    literal {
                     |      string_val: "Apache%"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometLike with custom escape character returns None") {
    val like = Like(Literal("Apache Spark"), Literal("Apache%"), '^')
    val proto = CometLike.convert(like, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometRLike with valid pattern") {
    val rlike = RLike(Literal("Apache123"), Literal("Apache[0-9]+"))
    val proto = CometRLike.convert(rlike, Seq.empty, binding = true)
    val expected = """rlike {
                     |  left {
                     |    literal {
                     |      string_val: "Apache123"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  right {
                     |    literal {
                     |      string_val: "Apache[0-9]+"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometRLike with case-insensitive flag returns None") {
    val rlike = RLike(Literal("Apache123"), Literal("(?i)apache[0-9]+"))
    val proto = CometRLike.convert(rlike, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometRLike with case-sensitive flag returns None") {
    val rlike = RLike(Literal("Apache123"), Literal("(?-i)apache[0-9]+"))
    val proto = CometRLike.convert(rlike, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometRLike with non-literal pattern returns None") {
    val rlike = RLike(Literal("Apache123"), AttributeReference("pattern", StringType, true)())
    val proto = CometRLike.convert(rlike, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometOctetLength") {
    val octetLength = OctetLength(Literal("Apache"))
    val proto = CometOctetLength.convert(octetLength, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "octet_length"
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometReverse") {
    val reverse = Reverse(Literal("Apache"))
    val proto = CometReverse.convert(reverse, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "reverse"
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringRPad with space padding") {
    val stringRPad = StringRPad(Literal("Apache"), Literal(10), Literal(" "))
    val proto = CometStringRPad.convert(stringRPad, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "rpad"
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      int_val: 10
                     |      datatype {
                     |        type_id: INT32
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringRPad with non-space padding returns None") {
    val stringRPad = StringRPad(Literal("Apache"), Literal(10), Literal("x"))
    val proto = CometStringRPad.convert(stringRPad, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometStringRPad with non-literal length returns None") {
    val stringRPad = StringRPad(
      Literal("Apache"),
      AttributeReference("len", DataTypes.IntegerType, true)(),
      Literal(" "))
    val proto = CometStringRPad.convert(stringRPad, Seq.empty, binding = true)
    assert(proto.isEmpty)
  }

  test("CometAscii") {
    val ascii = Ascii(Literal("A"))
    val proto = CometAscii.convert(ascii, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "ascii"
                     |  args {
                     |    literal {
                     |      string_val: "A"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometBitLength") {
    val bitLength = BitLength(Literal("test"))
    val proto = CometBitLength.convert(bitLength, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "bit_length"
                     |  args {
                     |    literal {
                     |      string_val: "test"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometConcatWs") {
    val concatWs = ConcatWs(Seq(Literal(","), Literal("a"), Literal("b")))
    val proto = CometConcatWs.convert(concatWs, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "concat_ws"
                     |  args {
                     |    literal {
                     |      string_val: ","
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "a"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "b"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringInstr") {
    val stringInstr = StringInstr(Literal("Apache Spark"), Literal("Spark"))
    val proto = CometStringInstr.convert(stringInstr, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "strpos"
                     |  args {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringReplace") {
    val stringReplace = StringReplace(Literal("Apache Spark"), Literal("Spark"), Literal("Comet"))
    val proto = CometStringReplace.convert(stringReplace, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "replace"
                     |  args {
                     |    literal {
                     |      string_val: "Apache Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "Spark"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "Comet"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTranslate") {
    val stringTranslate = StringTranslate(Literal("Apache"), Literal("ae"), Literal("XY"))
    val proto = CometStringTranslate.convert(stringTranslate, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "translate"
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "ae"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "XY"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometLength") {
    val length = Length(Literal("Apache"))
    val proto = CometLength.convert(length, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "length"
                     |  args {
                     |    literal {
                     |      string_val: "Apache"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringRepeat") {
    val stringRepeat = StringRepeat(Literal("test"), Literal(3))
    val proto = CometStringRepeat.convert(stringRepeat, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "repeat"
                     |  args {
                     |    literal {
                     |      string_val: "test"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      long_val: 3
                     |      datatype {
                     |        type_id: INT64
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTrim") {
    val stringTrim = StringTrim(Literal(" test "))
    val proto = CometStringTrim.convert(stringTrim, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "trim"
                     |  args {
                     |    literal {
                     |      string_val: " test "
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTrimLeft") {
    val stringTrimLeft = StringTrimLeft(Literal(" test "))
    val proto = CometStringTrimLeft.convert(stringTrimLeft, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "ltrim"
                     |  args {
                     |    literal {
                     |      string_val: " test "
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTrimRight") {
    val stringTrimRight = StringTrimRight(Literal(" test "))
    val proto = CometStringTrimRight.convert(stringTrimRight, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "rtrim"
                     |  args {
                     |    literal {
                     |      string_val: " test "
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTrimBoth") {
    val stringTrimBoth = new StringTrimBoth(Literal(" test "))
    val proto = CometStringTrimBoth.convert(stringTrimBoth, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "btrim"
                     |  args {
                     |    cast {
                     |      child {
                     |        scalarFunc {
                     |          func: "trim"
                     |          args {
                     |            literal {
                     |              string_val: " test "
                     |              datatype {
                     |                type_id: STRING
                     |              }
                     |            }
                     |          }
                     |        }
                     |      }
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |      timezone: "UTC"
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometStringTrim with trim character") {
    val stringTrim = StringTrim(Literal("xxtestxx"), Literal("x"))
    val proto = CometStringTrim.convert(stringTrim, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "trim"
                     |  args {
                     |    literal {
                     |      string_val: "xxtestxx"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |  args {
                     |    literal {
                     |      string_val: "x"
                     |      datatype {
                     |        type_id: STRING
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }

  test("CometUpper") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "false") {
      val upper = Upper(Literal("apache"))
      val proto = CometUpper.convert(upper, Seq.empty, binding = true)
      assert(proto.isEmpty)
    }
  }

  test("CometLower") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "false") {
      val lower = Lower(Literal("APACHE"))
      val proto = CometLower.convert(lower, Seq.empty, binding = true)
      assert(proto.isEmpty)
    }
  }

  test("CometInitCap") {
    withSQLConf(CometConf.COMET_EXEC_INITCAP_ENABLED.key -> "false") {
      val initCap = InitCap(Literal("apache spark"))
      val proto = CometInitCap.convert(initCap, Seq.empty, binding = true)
      assert(proto.isEmpty)
    }
  }

  test("CometUpper with config enabled") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      val upper = Upper(Literal("apache"))
      val proto = CometUpper.convert(upper, Seq.empty, binding = true)
      val expected = """scalarFunc {
                       |  func: "upper"
                       |  args {
                       |    literal {
                       |      string_val: "apache"
                       |      datatype {
                       |        type_id: STRING
                       |      }
                       |    }
                       |  }
                       |}
                       |""".stripMargin
      assert(proto.get.toString == expected)
    }
  }

  test("CometLower with config enabled") {
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      val lower = Lower(Literal("APACHE"))
      val proto = CometLower.convert(lower, Seq.empty, binding = true)
      val expected = """scalarFunc {
                       |  func: "lower"
                       |  args {
                       |    literal {
                       |      string_val: "APACHE"
                       |      datatype {
                       |        type_id: STRING
                       |      }
                       |    }
                       |  }
                       |}
                       |""".stripMargin
      assert(proto.get.toString == expected)
    }
  }

  test("CometInitCap with config enabled") {
    withSQLConf(CometConf.COMET_EXEC_INITCAP_ENABLED.key -> "true") {
      val initCap = InitCap(Literal("apache spark"))
      val proto = CometInitCap.convert(initCap, Seq.empty, binding = true)
      val expected = """scalarFunc {
                       |  func: "initcap"
                       |  args {
                       |    literal {
                       |      string_val: "apache spark"
                       |      datatype {
                       |        type_id: STRING
                       |      }
                       |    }
                       |  }
                       |}
                       |""".stripMargin
      assert(proto.get.toString == expected)
    }
  }

  test("CometChr") {
    val chr = Chr(Literal(65))
    val proto = CometChr.convert(chr, Seq.empty, binding = true)
    val expected = """scalarFunc {
                     |  func: "chr"
                     |  args {
                     |    literal {
                     |      int_val: 65
                     |      datatype {
                     |        type_id: INT32
                     |      }
                     |    }
                     |  }
                     |}
                     |""".stripMargin
    assert(proto.get.toString == expected)
  }
}
