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

import org.apache.comet.serde.{Compatible, Incompatible, SupportLevel}

/**
 * Regex flavor for [[CometRegex]]. The first version only implements [[RegexFlavor.RLike]]; later
 * flavors (for example `regexp_replace` / `split`) can add extra reject rules such as empty-match
 * divergence without changing the scanner's whitelist core.
 */
sealed trait RegexFlavor

object RegexFlavor {
  case object RLike extends RegexFlavor
}

/**
 * Plan-time whitelist analyzer for literal Java regex patterns. A pattern is [[Compatible]] only
 * when every construct is one the analyzer positively recognizes as equivalent on Spark's
 * `java.util.regex` engine and Comet's Rust `regex` crate. Anything unrecognized is
 * [[Incompatible]]: the safe direction, so a missed construct never silently takes the native
 * path.
 *
 * This is a recursive-descent scan, not a search for forbidden substrings. `[(?=]` is a character
 * class of literals, not a lookahead; `\\d` is a literal backslash plus `d`, not a digit class.
 */
object CometRegex {

  def supportLevel(pattern: String, flavor: RegexFlavor = RegexFlavor.RLike): SupportLevel = {
    flavor match {
      case RegexFlavor.RLike =>
        val scanner = new Scanner(pattern)
        if (scanner.parseExpr().isDefined && !scanner.remaining) {
          Compatible()
        } else {
          Incompatible(None)
        }
    }
  }

  private val MetaEscapes: Set[Char] =
    Set('.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '^', '$', '\\')

  // Conservative structural heuristics for native admission. These are not a
  // compiled-byte budget and do not map directly to the Rust `regex` crate's
  // default 10 MiB compiled-program size limit. Staying under these caps does
  // not prove that `Regex::new` will succeed; exceeding one keeps the pattern
  // on the JVM dispatcher.
  //
  // The values stay well below crate defaults and reject shapes reproduced
  // during review as compiled-size or nesting-depth failures:
  // `a{1000000}`, `[^;]{20000}`, counted `(a{100})` wrapped `{100}`,
  // unbounded `{0,}` of `[^;]{256}` wrapped `{256}`, 251 nested groups, and
  // nested `*` wrappers whose structural expansion stays under MaxExpansion
  // but whose compiled program exceeds 10 MiB, and capturing groups copied
  // by counted repetition. The `regex-syntax` default nesting limit is 250.
  //
  // Raising a cap does not mean the resulting patterns remain below 10 MiB.
  // Re-check `Regex::new` against these pattern classes before changing one.
  //
  // MaxGroupDepth caps group nesting at 32.
  // MaxCountedBound caps each numeric `{n}`, `{n,}`, or `{n,m}` bound at 256.
  // MaxExpansion caps the aggregate structural estimate at 4096 across
  // concatenation, alternation, and counted repetition. Unbounded `{n,}` uses
  // a multiplier of at least 1 so `{0,}` cannot hide its inner compile cost.
  // MaxQuantifierNesting caps stacked `*` / `+` / `?` / `{n}` wrappers. Deeply
  // nested quantified expressions can substantially increase Rust regex
  // compilation state without exceeding the structural expansion estimate
  // (nullable-state cost). Keep such patterns on the JVM.
  // CaptureStateCost counts CaptureStart and CaptureEnd per capturing group.
  // Counted repetition copies those states with the inner expression; `*`,
  // `+`, and `?` do not, and `(?:...)` does not add capture states. This is
  // still a structural heuristic, not a compiled-byte proof.
  private val MaxGroupDepth = 32
  private val MaxCountedBound = 256
  private val MaxExpansion = 4096L
  private[expressions] val MaxQuantifierNesting = 8
  private val CaptureStateCost = 2L

  private case class Parsed(size: Long, qNest: Int, captures: Int)

  private class Scanner(pattern: String) {
    private var i = 0
    private var groupDepth = 0

    def remaining: Boolean = i < pattern.length

    private def peek: Char = pattern.charAt(i)

    private def peekOffset(n: Int): Option[Char] = {
      val idx = i + n
      if (idx < pattern.length) Some(pattern.charAt(idx)) else None
    }

    private def consume(): Char = {
      val c = peek
      i += 1
      c
    }

    private def startsWith(s: String): Boolean = pattern.startsWith(s, i)

    // Returns a conservative compiled-size estimate, or None if the construct
    // is unrecognized or exceeds the compile budget.
    def parseExpr(): Option[Long] = parseExprParsed().map(_.size)

    private def parseExprParsed(): Option[Parsed] = {
      val first = parseTerm() match {
        case Some(s) => s
        case None => return None
      }
      var total = first
      while (remaining && peek == '|') {
        consume()
        parseTerm() match {
          case Some(s) =>
            addWithinBudget(total.size, s.size) match {
              case Some(next) =>
                total = Parsed(next, math.max(total.qNest, s.qNest), total.captures + s.captures)
              case None => return None
            }
          case None => return None
        }
      }
      Some(total)
    }

    private def parseTerm(): Option[Parsed] = {
      var totalSize = 0L
      var qNest = 0
      var captures = 0
      var any = false
      while (remaining && peek != '|' && peek != ')') {
        parseFactor() match {
          case Some(s) =>
            addWithinBudget(totalSize, s.size) match {
              case Some(next) =>
                totalSize = next
                qNest = math.max(qNest, s.qNest)
                captures += s.captures
                any = true
              case None => return None
            }
          case None =>
            return None
        }
      }
      Some(if (any) Parsed(totalSize, qNest, captures) else Parsed(1L, 0, 0))
    }

    private def parseFactor(): Option[Parsed] = {
      val atom = parseAtom() match {
        case Some(s) => s
        case None => return None
      }
      parseOptionalQuantifier(atom)
    }

    private def parseAtom(): Option[Parsed] = {
      if (!remaining) {
        return None
      }
      peek match {
        case '\\' =>
          if (parseEscape(inClass = false).isDefined) Some(Parsed(1L, 0, 0)) else None
        case '[' =>
          if (parseClass()) Some(Parsed(1L, 0, 0)) else None
        case '(' => parseGroup()
        case '.' | '^' | '$' | '*' | '+' | '?' | '{' | '}' | ')' | ']' | '|' =>
          None
        case c if isPrintableAscii(c) =>
          consume()
          Some(Parsed(1L, 0, 0))
        case _ => None
      }
    }

    private def parseGroup(): Option[Parsed] = {
      consume() // '('
      if (!remaining) {
        return None
      }
      val capturing = if (startsWith("?:")) {
        i += 2
        false
      } else if (peek == '?') {
        // lookaround, flags, named groups, atomic groups, comments, ...
        return None
      } else {
        true
      }
      if (groupDepth >= MaxGroupDepth) {
        return None
      }
      groupDepth += 1
      val inner = parseExprParsed()
      val closed = remaining && consume() == ')'
      groupDepth -= 1
      if (!closed) {
        None
      } else if (capturing) {
        inner.map(p => Parsed(p.size, p.qNest, p.captures + 1))
      } else {
        inner
      }
    }

    private def parseOptionalQuantifier(inner: Parsed): Option[Parsed] = {
      if (!remaining) {
        return Some(inner)
      }
      peek match {
        case '*' | '+' | '?' =>
          consume()
          if (remaining && (peek == '+' || peek == '?')) {
            // possessive or lazy
            None
          } else {
            applyQuantifier(inner, inner.size)
          }
        case '{' => parseCountedQuantifier(inner)
        case _ => Some(inner)
      }
    }

    private def applyQuantifier(inner: Parsed, size: Long): Option[Parsed] = {
      val qNest = inner.qNest + 1
      if (qNest > MaxQuantifierNesting) {
        None
      } else {
        Some(Parsed(size, qNest, inner.captures))
      }
    }

    private def parseCountedQuantifier(inner: Parsed): Option[Parsed] = {
      consume() // '{'
      val n = parseNonNegInt() match {
        case Some(v) => v
        case None => return None
      }
      if (n > MaxCountedBound) {
        return None
      }
      if (!remaining) {
        return None
      }
      val bound = peek match {
        case '}' =>
          consume()
          if (isLazyOrPossessiveSuffix) {
            return None
          }
          n
        case ',' =>
          consume()
          if (!remaining) {
            return None
          }
          if (peek == '}') {
            consume()
            if (isLazyOrPossessiveSuffix) {
              return None
            }
            math.max(1, n)
          } else {
            val m = parseNonNegInt() match {
              case Some(v) => v
              case None => return None
            }
            if (m < n || m > MaxCountedBound) {
              return None
            }
            if (!(remaining && consume() == '}' && !isLazyOrPossessiveSuffix)) {
              return None
            }
            m
          }
        case _ =>
          return None
      }
      val copyCost =
        saturatingAdd(inner.size, saturatingMul(CaptureStateCost, inner.captures.toLong))
      multiplyWithinBudget(copyCost, bound.toLong).flatMap { size =>
        applyQuantifier(inner, size)
      }
    }

    private def addWithinBudget(a: Long, b: Long): Option[Long] = {
      val total = saturatingAdd(a, b)
      if (total > MaxExpansion) None else Some(total)
    }

    private def multiplyWithinBudget(a: Long, b: Long): Option[Long] = {
      // A zero-count repetition still contributes syntax and compile work.
      // Keep every factor visible to aggregate term/alternation accounting.
      val total = math.max(1L, saturatingMul(a, b))
      if (total > MaxExpansion) None else Some(total)
    }

    private def saturatingAdd(a: Long, b: Long): Long = {
      val s = a + b
      if (s < 0) Long.MaxValue else s
    }

    private def saturatingMul(a: Long, b: Long): Long = {
      if (a != 0 && b > Long.MaxValue / a) {
        Long.MaxValue
      } else {
        a * b
      }
    }

    private def isLazyOrPossessiveSuffix: Boolean =
      remaining && (peek == '+' || peek == '?')

    private def parseNonNegInt(): Option[Int] = {
      if (!remaining || !isAsciiDigit(peek)) {
        return None
      }
      var v = 0L
      while (remaining && isAsciiDigit(peek)) {
        v = v * 10 + (consume() - '0')
        if (v > Int.MaxValue) {
          return None
        }
      }
      Some(v.toInt)
    }

    private def parseClass(): Boolean = {
      consume() // '['
      if (remaining && peek == '^') {
        consume()
      }
      var contentStarted = false
      var lastAtom: Option[Char] = None
      while (remaining && !(peek == ']' && contentStarted)) {
        // Rust class set ops (&& / ~~ / --) are not Java literals. Nested
        // classes and unescaped class delimiters as atoms are also out of subset.
        if (startsWith("&&") || startsWith("~~") || startsWith("--") || peek == '[') {
          return false
        }
        val ranging = lastAtom.isDefined && peek == '-' && peekOffset(1).exists(_ != ']')
        if (ranging) {
          consume() // '-'
          parseClassAtom() match {
            case Some(end) if end >= lastAtom.get =>
              lastAtom = None
            case _ =>
              return false
          }
        } else {
          parseClassAtom() match {
            case Some(c) =>
              lastAtom = Some(c)
              contentStarted = true
            case None =>
              return false
          }
        }
      }
      remaining && consume() == ']'
    }

    private def parseClassAtom(): Option[Char] = {
      if (!remaining) {
        return None
      }
      // Unescaped `]` is only the class closer, never a range endpoint. An
      // unescaped `[` starts a nested class in Java, so it cannot be a literal
      // range endpoint even though Rust accepts it as one.
      if (peek == ']' || peek == '[') {
        return None
      }
      if (peek == '\\') {
        parseEscape(inClass = true)
      } else if (isPrintableAscii(peek)) {
        Some(consume())
      } else {
        None
      }
    }

    private def parseEscape(inClass: Boolean): Option[Char] = {
      consume() // '\'
      if (!remaining) {
        return None
      }
      val c = peek
      val allowed = MetaEscapes.contains(c) || (inClass && c == '-')
      if (allowed) {
        Some(consume())
      } else {
        None
      }
    }

    private def isPrintableAscii(c: Char): Boolean = c >= 0x20 && c <= 0x7e

    private def isAsciiDigit(c: Char): Boolean = c >= '0' && c <= '9'
  }
}
