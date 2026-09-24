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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Generates the Java oracle for Comet's admitted RLIKE subset. Run with JDK 17. */
public class GenerateRegexFixtures {
  private static final List<String> CASES = new ArrayList<>();

  private static String quote(String value) {
    StringBuilder result = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '"' || c == '\\') {
        result.append('\\').append(c);
      } else if (c < 0x20 || c > 0x7e) {
        result.append(String.format("\\u%04x", (int) c));
      } else {
        result.append(c);
      }
    }
    return result.append('"').toString();
  }

  private static void add(String category, String pattern, String... subjects) {
    Pattern compiled = Pattern.compile(pattern);
    for (String subject : subjects) {
      boolean expected = compiled.matcher(subject).find();
      CASES.add(
          "    {\"category\": "
              + quote(category)
              + ", \"pattern\": "
              + quote(pattern)
              + ", \"subject\": "
              + quote(subject)
              + ", \"expected\": "
              + expected
              + "}");
    }
  }

  private static void addRange(String start, char lo, String end, char hi) {
    // Probe both endpoints, an interior point, and the immediately adjacent nonmembers.
    for (String prefix : new String[] {"[", "[^"}) {
      add(
          "escaped-range-boundaries",
          prefix + start + "-" + end + "]",
          "",
          String.valueOf((char) (lo - 1)),
          String.valueOf(lo),
          String.valueOf((char) ((lo + hi) / 2)),
          String.valueOf(hi),
          String.valueOf((char) (hi + 1)),
          "\n",
          "\ud83d\ude00");
    }
  }

  private static void addRangeMatrix() {
    // Every escape admitted by parseEscape(inClass = true), including the class-only hyphen.
    char[] escapes = ".*+?()[]{}|^$\\-".toCharArray();
    for (char start : escapes) {
      addRange("\\" + start, start, "~", '~');
      addRange("!", '!', "\\" + start, start);
      for (char end : escapes) {
        if (start <= end) {
          addRange("\\" + start, start, "\\" + end, end);
        }
      }
    }
  }

  private static void addQuantifierMatrix() {
    String[] quantifiers = {"*", "+", "?", "{0}", "{1}", "{2}", "{0,}", "{1,}", "{1,2}"};
    for (String group : new String[] {"(", "(?:"}) {
      // All ordered pairs, including nullable bodies. Required surrounding literals prevent
      // find() from skipping the repeated input and matching only the trailing delimiter.
      for (String atom : new String[] {"a", "a|"}) {
        for (String inner : quantifiers) {
          for (String outer : quantifiers) {
            String pattern = "c" + group + group + atom + ")" + inner + ")" + outer + "b";
            add(
                "nested-quantifier-pairs",
                pattern,
                "", "c", "ca", "cb", "cab", "caab", "caaaab", "caac", "xcabx");
          }
        }
      }
      // Near-limit depths exercise real compilation, not only scanner admission. Keep subjects
      // short for ambiguous unbounded repetitions to bound Java backtracking work.
      for (String quantifier : quantifiers) {
        for (int depth : new int[] {1, 2, 7, 8}) {
          String pattern = group.repeat(depth) + "a" + (")" + quantifier).repeat(depth);
          add("quantifier-depth-matrix", pattern, "", "a", "aa", "b");
          if (quantifier.equals("{2}")) {
            int minimum = 1 << depth;
            add("nested-counted-boundaries", pattern, "a".repeat(minimum - 1), "a".repeat(minimum));
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1 || Runtime.version().feature() != 17) {
      throw new IllegalArgumentException(
          "Run with JDK 17: java dev/GenerateRegexFixtures.java OUTPUT");
    }
    String[] subjects = {
      "", "abc", "abc123", "ABC", "foo", "bar", "foobar", "xxbarxx", "a+b", "\\d", "a", "b", "aa",
      "aaaa", "ab", "abab", "ac", "cd", "abcd", "xxabbxx", "def", "123", "(?=", ".", "-", "z",
      "a b", "@", "[", "]", "A", "_", "~", "a-z",
      // Escaped so the output does not depend on the JDK 17 default source encoding.
      "\u03b1\u03b2\u03b3", "\u0661\u0662\u0663", "\u4f60\u597d", "\ud83d\ude00", "e\u0301",
      "a\ud83d\ude00b", "\n",
      "\r", "\r\n", "\t", "\u000b", "\f", "\u0000", "\u007f", "\u00a0", "\ufeff", "\u0085",
      "\u2028", "\u2029", "\nabc", "abc\n", "\nabc\n"
    };
    String[][] groups = {
      {"literal", "abc", "a b"},
      {
        "class",
        "[0-9_]",
        "[^0-9]",
        "[a-zA-Z_][a-zA-Z0-9_]*",
        "[^a]",
        "[^;]+",
        "[a-]",
        "[-a]",
        "[a\\-z]",
        "[@-\\[]",
        "[\\.-9]",
        "[\\--/]",
        "[\\\\-a]",
        "[a~b]",
        "[.]",
        "[\\]a]"
      },
      {"lexer", "[(?=]", "\\(\\?=", "\\\\d", "a\\+b"},
      {"group", "(foo)", "(?:foo|bar)", "abc|def", "(?:(?:foo)|bar)"},
      {"quantifier", "a*", "a+", "a?", "a{2}", "a{2,}", "a{2,4}"},
      {
        "composition",
        "abc[0-9]+",
        "(foo|bar){1,3}",
        "(ab)+",
        "(a{2}){3}",
        "(?:ab|cd){2,3}",
        "a(?:b|c)+d?",
        "([a-c]+|[0-9]{2})_?",
        "(?:a?b)*"
      },
      {
        "empty",
        "",
        "()",
        "(?:)",
        "a|",
        "|a",
        "a||b",
        "a{0}",
        "a{0,}",
        "a{0,2}",
        "(?:a|){2}",
        "(?:){2}"
      }
    };
    for (String[] group : groups) {
      for (int i = 1; i < group.length; i++) {
        add(group[0], group[i], subjects);
      }
    }
    for (char c : ".*+?()[]{}|^$\\".toCharArray()) {
      add("escape", "\\" + c, "", String.valueOf(c), "x" + c + "y", "abc");
      add("class-escape", "[\\" + c + "]", "", String.valueOf(c), "abc");
    }
    // Finite compositions exercise grammar interactions without random or unbounded generation.
    for (String atom : new String[] {"a", "[a-c]", "[^;]", "(?:ab|c)", "(a)"}) {
      for (String quantifier : new String[] {"", "*", "+", "?", "{0}", "{2}", "{1,}", "{1,3}"}) {
        add("generated-composition", "(?:" + atom + quantifier + ")b|cd", subjects);
      }
    }
    addRangeMatrix();
    addQuantifierMatrix();
    add("group-depth-32", "(".repeat(32) + "a" + ")".repeat(32), "", "a", "b");
    add("counted-bound-256", "a{256}", "", "a".repeat(255), "a".repeat(256));
    add("quantifier-depth-8", "(?:".repeat(8) + "a" + "){1}".repeat(8), "", "a", "b");
    // Unlike nested {1}, stacked stars exercise nullable compilation state.
    add("star-depth-8", "(".repeat(8) + "a" + ")*".repeat(8), "", "a", "aa", "b");
    add("expansion-4095", "(?:" + "a".repeat(16) + "){255}" + "a".repeat(15), "", "b");
    add("expansion-4096", "(?:" + "a".repeat(16) + "){256}", "", "b", "a".repeat(4096));
    add("capture-expansion-4096", "(" + "a".repeat(14) + "){256}", "", "b");
    add("class-expansion-4096", "[abcdefghijklmnop]{256}", "", "z", "a".repeat(256));
    String json =
        "{\n  \"jdk_vendor\": "
            + quote(System.getProperty("java.vendor"))
            + ",\n  \"jdk_runtime_version\": "
            + quote(System.getProperty("java.runtime.version"))
            + ",\n  \"cases\": [\n"
            + String.join(",\n", CASES)
            + "\n  ]\n}\n";
    Files.writeString(Path.of(args[0]), json, StandardCharsets.UTF_8);
  }
}
