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

package org.apache.comet.coverage

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.regex.{Pattern, PatternSyntaxException}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.io.Source
import scala.util.Using
import scala.util.control.NonFatal

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.expressions.{CometRegex, RegexFlavor}
import org.apache.comet.serde.{Compatible, Incompatible}

/** Offline component measurement. Does not start Spark or invoke the native regex kernel. */
object RegexCoverage {
  private val mapper = new ObjectMapper()

  private def sparkLiteral(sql: String, escaped: Boolean): String = {
    val conf = new SQLConf
    conf.setConfString("spark.sql.parser.escapedStringLiterals", escaped.toString)
    conf.setConfString("spark.sql.ansi.enabled", "false")
    conf.setConfString("spark.sql.ansi.doubleQuotedIdentifiers", "false")
    SQLConf.withExistingConf(conf) {
      CatalystSqlParser.parseExpression(sql) match {
        case Literal(value: UTF8String, _: StringType) => value.toString
        case _ => throw new IllegalArgumentException("expected one non-null string literal")
      }
    }
  }

  private def admitted(pattern: String): Boolean = {
    CometRegex.supportLevel(pattern, RegexFlavor.RLike) match {
      case _: Compatible => true
      case _: Incompatible => false
      case other => throw new IllegalStateException(s"unexpected support level: $other")
    }
  }

  private def javaValid(pattern: String): Boolean = {
    try {
      Pattern.compile(pattern)
      true
    } catch {
      case _: PatternSyntaxException => false
    }
  }

  private def evaluate(index: Int, decoder: String, value: String): ObjectNode = {
    require(Set("python-value", "spark-literal", "hive-literal-crosscheck").contains(decoder))
    val result = mapper.createObjectNode()
    result.put("input_index", index)
    result.put("decoder", decoder)
    val pattern = try {
      decoder match {
        case "python-value" => value
        case "spark-literal" => sparkLiteral(value, escaped = false)
        case "hive-literal-crosscheck" =>
          require(value.length >= 2 && Set('\'', '"').contains(value.head) &&
            value.last == value.head, "unsupported Hive literal spelling")
          val hive = BaseSemanticAnalyzer.unescapeSQLString(value)
          if (hive != sparkLiteral(value, escaped = false)) {
            result.put("status", "literal_decoder_disagreement")
            return result
          }
          result.put("hive_matches_spark_default", true)
          hive
      }
    } catch {
      case NonFatal(error) =>
        result.put("status", "literal_decode_error")
        result.put("error_class", error.getClass.getName)
        return result
    }
    result.put("pattern_value", pattern)
    try {
      Pattern.compile(pattern)
    } catch {
      case error: PatternSyntaxException =>
        result.put("status", "invalid_java_regex")
        result.put("error_description", error.getDescription)
        return result
    }
    result.put("status", "eligible")
    result.put("admitted", admitted(pattern))
    if (decoder != "python-value") {
      val legacy = sparkLiteral(value, escaped = true)
      val valid = javaValid(legacy)
      val sensitivity = result.putObject("spark_escaped_literals_true")
      sensitivity.put("pattern_value", legacy)
      sensitivity.put("decoding_differs", legacy != pattern)
      sensitivity.put("java_valid", valid)
      if (valid) sensitivity.put("admitted", admitted(legacy))
    }
    result
  }

  private def selfTest(): Unit = {
    assert(sparkLiteral("""'\\d+'""", escaped = false) == """\d+""")
    assert(sparkLiteral("""r'\d+'""", escaped = false) == """\d+""")
    assert(sparkLiteral("\"^ab\"", escaped = false) == "^ab")
    assert(sparkLiteral("''", escaped = false).isEmpty)
    val boundary = evaluate(0, "spark-literal", """'\d'""")
    assert(boundary.get("pattern_value").asText() == "d")
    assert(boundary.get("admitted").asBoolean())
    assert(!boundary.get("spark_escaped_literals_true").get("admitted").asBoolean())
    assert(evaluate(0, "python-value", "[.]").get("admitted").asBoolean())
    assert(evaluate(0, "python-value", """\\d""").get("admitted").asBoolean())
    assert(!evaluate(0, "python-value", """\d""").get("admitted").asBoolean())
    assert(evaluate(0, "python-value", "[").get("status").asText() == "invalid_java_regex")
    assert(evaluate(0, "spark-literal", "NULL").get("status").asText() == "literal_decode_error")
    assert(evaluate(0, "hive-literal-crosscheck", """'\\d+'""")
      .get("hive_matches_spark_default").asBoolean())
    // Spark understands this Unicode escape; Hive's unescaper interprets it differently.
    assert(evaluate(0, "hive-literal-crosscheck", """'\U0001F600'""")
      .get("status").asText() == "literal_decoder_disagreement")
    assert(inspect(0, "(a){2}").get("structural_expansion").asLong() == 6L)
    assert(inspect(0, "[a-z]").get("structural_expansion").asLong() == 2L)
    assert(!inspect(0, "a{257}").has("structural_expansion"))
    println("RegexCoverage component checks passed")
  }

  private def origin(cls: Class[_]): String =
    cls.getProtectionDomain.getCodeSource.getLocation.toString

  // Read the existing scanner's estimate instead of implementing its cost model again.
  // This offline-only reflection deliberately fails if the private implementation changes.
  private def inspect(index: Int, pattern: String): ObjectNode = {
    val scannerClass = Class.forName(
      "org.apache.comet.expressions.CometRegex$Scanner", true, CometRegex.getClass.getClassLoader)
    val constructor = scannerClass.getDeclaredConstructor(classOf[String])
    constructor.setAccessible(true)
    val scanner = constructor.newInstance(pattern)
    val parse = scannerClass.getDeclaredMethod("parseExpr")
    parse.setAccessible(true)
    val remaining = scannerClass.getDeclaredMethod("remaining")
    remaining.setAccessible(true)
    val parsed = parse.invoke(scanner).asInstanceOf[Option[Any]]
    val complete = parsed.isDefined && !remaining.invoke(scanner).asInstanceOf[Boolean]
    val decision = admitted(pattern)
    require(complete == decision, "scanner inspection disagrees with supportLevel")
    val result = mapper.createObjectNode()
    result.put("input_index", index)
    result.put("admitted", decision)
    result.put("java_valid", javaValid(pattern))
    if (complete) result.put("structural_expansion", parsed.get.asInstanceOf[Long])
    result
  }

  def main(args: Array[String]): Unit = {
    if (args.sameElements(Array("--self-test"))) {
      selfTest()
      return
    }
    if (args.length == 3 && args(0) == "--inspect") {
      val results = Using.resource(Source.fromFile(args(1), "UTF-8")) { input =>
        input.getLines().map { line =>
          val request = mapper.readTree(line)
          inspect(request.get("input_index").asInt(), request.get("value").asText()).toString
        }.mkString("", "\n", "\n")
      }
      Files.write(Paths.get(args(2)), results.getBytes(UTF_8))
      return
    }
    require(args.length == 3, "expected: requests.jsonl decisions.jsonl runtime.json")
    val results = Using.resource(Source.fromFile(args(0), "UTF-8")) { input =>
      input.getLines().map { line =>
        val request = mapper.readTree(line)
        evaluate(request.get("input_index").asInt(), request.get("decoder").asText(),
          request.get("value").asText()).toString
      }.mkString("", "\n", "\n")
    }
    Files.write(Paths.get(args(1)), results.getBytes(UTF_8))
    val runtime = mapper.createObjectNode()
    runtime.put("spark_version", org.apache.spark.SPARK_VERSION)
    runtime.put("scala_version", scala.util.Properties.versionNumberString)
    runtime.put("java_version", System.getProperty("java.version"))
    runtime.put("analyzer_origin", origin(CometRegex.getClass))
    runtime.put("support_level_origin", origin(classOf[Compatible]))
    runtime.put("spark_parser_origin", origin(CatalystSqlParser.getClass))
    runtime.put("hive_decoder_origin", origin(classOf[BaseSemanticAnalyzer]))
    Files.write(Paths.get(args(2)), (runtime.toString + "\n").getBytes(UTF_8))
  }
}
