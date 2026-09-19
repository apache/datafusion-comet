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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.serde.CometRLike

class GenerateDocsSuite extends AnyFunSuite {

  private val configKey = "spark.comet.expression.RLike.allowIncompatible"

  test("conditional-native expressions describe both default routing paths") {
    assert(CometRLike.hasConditionalNativeDefault)

    val markdown = GenerateDocs.renderExpressionCompatNotes(
      Seq(GenerateDocs.ExprNotes(
        "RLike",
        CometRLike.getCompatibleNotes(),
        CometRLike.getIncompatibleReasons(),
        Seq.empty,
        nativeOptIn = true,
        nativeOptInConfigKey = configKey,
        conditionalNativeDefault = true,
        codegenDispatchFallback = false)))

    val expected =
      s"""
         |## RLike
         |
         |The following cases use Comet's native implementation by default:
         |
         |- A `UTF8_BINARY` literal pattern admitted by the [plan-time compatibility analyzer](../../regex.md#when-the-rust-engine-is-safe) is evaluated natively by default.
         |
         |For applicable cases that are not selected for native execution automatically, `RLike` is evaluated in the JVM using Spark's own code-generated implementation (run inside the Comet pipeline) by default. Set `$configKey=true` to explicitly select Comet's native implementation, which has the following differences from Spark:
         |
         |- For applicable literal patterns outside the automatically admitted subset, the native Rust regex engine may behave differently from Java regex.
         |""".stripMargin

    assert(markdown == expected)
    assert(!markdown.contains("By default, `RLike` is evaluated in the JVM"))
    assert(!markdown.contains("differences from Spark are always present"))
    assert(
      markdown.contains(
        "[plan-time compatibility analyzer](../../regex.md#when-the-rust-engine-is-safe)"))
  }

  test("ordinary native opt-in expression output is unchanged") {
    val markdown = GenerateDocs.renderExpressionCompatNotes(
      Seq(GenerateDocs.ExprNotes(
        "OptInExpr",
        Seq.empty,
        Seq("Native difference."),
        Seq.empty,
        nativeOptIn = true,
        nativeOptInConfigKey = "spark.comet.expression.OptInExpr.allowIncompatible",
        conditionalNativeDefault = false,
        codegenDispatchFallback = false)))

    val expected =
      "\n## OptInExpr\n" +
        "\nBy default, `OptInExpr` is evaluated in the JVM using Spark's own code-generated" +
        " implementation (run inside the Comet pipeline), which matches Spark exactly." +
        " Set `spark.comet.expression.OptInExpr.allowIncompatible=true` to opt into Comet's" +
        " native implementation instead, which has the following differences from Spark:\n\n" +
        "- Native difference.\n"

    assert(markdown == expected)
  }

  test("ordinary expression headings are unchanged") {
    val markdown = GenerateDocs.renderExpressionCompatNotes(
      Seq(GenerateDocs.ExprNotes(
        "OrdinaryExpr",
        Seq("Always-present difference."),
        Seq("Incompatible case."),
        Seq("Unsupported case."),
        nativeOptIn = false,
        nativeOptInConfigKey = "spark.comet.expression.OrdinaryExpr.allowIncompatible",
        conditionalNativeDefault = false,
        codegenDispatchFallback = false)))

    assert(
      markdown.contains(
        "The following differences from Spark are always present and do not require any " +
          "additional configuration:"))
    assert(
      markdown.contains(
        "The following incompatibilities cause `OrdinaryExpr` to fall back to Spark by " +
          "default."))
    assert(
      markdown.contains(
        "The following cases are not supported by Comet and always fall back to Spark,"))
  }
}
