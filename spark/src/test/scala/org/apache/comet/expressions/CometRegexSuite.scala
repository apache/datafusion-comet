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

package org.apache.comet.expressions

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.serde.{Compatible, Incompatible}

class CometRegexSuite extends AnyFunSuite {

  private def assertCompatible(pattern: String): Unit = {
    val level = CometRegex.supportLevel(pattern, RegexFlavor.RLike)
    assert(level.isInstanceOf[Compatible], s"expected Compatible for [$pattern], got $level")
  }

  private def assertIncompatible(pattern: String): Unit = {
    val level = CometRegex.supportLevel(pattern, RegexFlavor.RLike)
    assert(level.isInstanceOf[Incompatible], s"expected Incompatible for [$pattern], got $level")
  }

  test("admits ASCII literals, classes, quantifiers, groups, and alternation") {
    Seq(
      "",
      "abc",
      "abc[0-9]+",
      "[a-zA-Z_][a-zA-Z0-9_]*",
      "(foo|bar){1,3}",
      "a\\+b",
      "[0-9_]",
      "[^0-9]",
      "(?:foo)",
      "(foo)",
      "abc|def",
      "a*",
      "a+",
      "a?",
      "a{2}",
      "a{2,}",
      "a{2,4}",
      "a|",
      "|a",
      "[a-]",
      "[-a]",
      "[a\\-z]",
      "a b",
      "(?:(?:foo)|bar)",
      "(ab)+",
      "[a~b]",
      "(a{2}){3}").foreach(assertCompatible)
  }

  test("admits lexer-boundary patterns that a substring search would misclassify") {
    Seq(
      "[(?=]", // class of literals, not lookahead
      "\\(\\?=", // escaped literal `(?=`
      "\\\\d", // literal backslash plus `d`
      "[.]" // class-local literal dot
    ).foreach(assertCompatible)
  }

  test("rejects constructs that diverge from Java regex or are unrecognized") {
    Seq(
      "\\d+",
      "\\w+",
      ".",
      "(?i)abc",
      "(?m)^abc$",
      "(foo)\\1",
      "foo(?=bar)",
      "(?<=foo)bar",
      "a*+",
      "(?>abc)",
      "\\p{L}+",
      "[a-z&&[^aeiou]]",
      "\\u0041",
      "\\012",
      "^abc",
      "abc$",
      "abc.",
      "[",
      "\\s+",
      "\\b",
      "\\B",
      "a+?",
      "a*?",
      "(?<name>foo)",
      "\\n",
      "\\t",
      "café",
      "你好").foreach(assertIncompatible)
  }

  test("rejects scanner edge cases the whitelist must not silently admit") {
    Seq(
      "\\A", // beginning of input; Java-only relative to the Rust crate
      "\\Z", // end of input, before final line terminator
      "\\z", // absolute end of input
      "\\G", // end of previous match
      "\\W",
      "\\S",
      "\\D",
      "a{2,1}", // inverted counted range
      "{,4}", // missing lower bound; `{` is not a valid atom
      "a{2", // unclosed counted quantifier
      "(", // unclosed group
      "(abc",
      "[z-a]", // inverted character-class range
      "[[a]]", // nested class
      "\\x41", // hex escape
      "\\Qabc\\E" // quoted span
    ).foreach(assertIncompatible)
  }

  test("rejects Rust-only character-class set operations") {
    Seq("[a~~b]", "[a-z--b]", "[^a~~b]", "[^a-z--b]", "[a&&b]").foreach(assertIncompatible)
  }

  test("rejects unescaped ] used as a character-class atom or range endpoint") {
    Seq("[]-a]", "[^]-a]", "[]]", "[^]]").foreach(assertIncompatible)
    assertCompatible("[\\]]")
    assertCompatible("[a\\]]")
  }

  test("rejects counted or nested patterns that can exceed the Rust compile budget") {
    Seq("a{1000000}", "[^;]{20000}", "a{257}", "(a{100}){100}", "(" * 33 + "a" + ")" * 33)
      .foreach(assertIncompatible)
    assertCompatible("a{256}")
    assertCompatible("(a{2}){3}")
    assertCompatible("(" * 32 + "a" + ")" * 32)
  }
}
