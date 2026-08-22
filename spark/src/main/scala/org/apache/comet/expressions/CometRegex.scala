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
        if (scanner.parseExpr() && !scanner.remaining) {
          Compatible()
        } else {
          Incompatible(None)
        }
    }
  }

  private val MetaEscapes: Set[Char] =
    Set('.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '^', '$', '\\')

  private class Scanner(pattern: String) {
    private var i = 0

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

    def parseExpr(): Boolean = {
      if (!parseTerm()) {
        return false
      }
      while (remaining && peek == '|') {
        consume()
        if (!parseTerm()) {
          return false
        }
      }
      true
    }

    private def parseTerm(): Boolean = {
      while (remaining && peek != '|' && peek != ')') {
        if (!parseFactor()) {
          return false
        }
      }
      true
    }

    private def parseFactor(): Boolean = {
      if (!parseAtom()) {
        return false
      }
      parseOptionalQuantifier()
    }

    private def parseAtom(): Boolean = {
      if (!remaining) {
        return false
      }
      peek match {
        case '\\' => parseEscape(inClass = false).isDefined
        case '[' => parseClass()
        case '(' => parseGroup()
        case '.' | '^' | '$' | '*' | '+' | '?' | '{' | '}' | ')' | ']' | '|' =>
          false
        case c if isPrintableAscii(c) =>
          consume()
          true
        case _ => false
      }
    }

    private def parseGroup(): Boolean = {
      consume() // '('
      if (!remaining) {
        return false
      }
      if (startsWith("?:")) {
        i += 2
      } else if (peek == '?') {
        // lookaround, flags, named groups, atomic groups, comments, ...
        return false
      }
      if (!parseExpr()) {
        return false
      }
      remaining && consume() == ')'
    }

    private def parseOptionalQuantifier(): Boolean = {
      if (!remaining) {
        return true
      }
      peek match {
        case '*' | '+' | '?' =>
          consume()
          if (remaining && (peek == '+' || peek == '?')) {
            // possessive or lazy
            false
          } else {
            true
          }
        case '{' => parseCountedQuantifier()
        case _ => true
      }
    }

    private def parseCountedQuantifier(): Boolean = {
      consume() // '{'
      val n = parseNonNegInt() match {
        case Some(v) => v
        case None => return false
      }
      if (!remaining) {
        return false
      }
      peek match {
        case '}' =>
          consume()
          !isLazyOrPossessiveSuffix
        case ',' =>
          consume()
          if (!remaining) {
            return false
          }
          if (peek == '}') {
            consume()
            !isLazyOrPossessiveSuffix
          } else {
            val m = parseNonNegInt() match {
              case Some(v) => v
              case None => return false
            }
            if (m < n) {
              return false
            }
            remaining && consume() == '}' && !isLazyOrPossessiveSuffix
          }
        case _ => false
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
        if (startsWith("&&") || peek == '[') {
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
