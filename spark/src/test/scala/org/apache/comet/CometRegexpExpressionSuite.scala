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

class CometRegexpExpressionSuite extends CometTestBase {

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
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\\\d+)-(\\\\d+)', 0) FROM tbl")
        // Test group 1
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\\\d+)-(\\\\d+)', 1) FROM tbl")
        // Test group 2
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\\\d+)-(\\\\d+)', 2) FROM tbl")
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
          "SELECT regexp_extract(_1, '(\\\\d{3}-\\\\d{3}-\\\\d{4})', 1) FROM tbl")
        // Extract price
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '\\\\$(\\\\d+\\\\.\\\\d+)', 1) FROM tbl")
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
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '\\\\d+', 0) FROM tbl")
        // Test with explicit group 0
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)', 0) FROM tbl")
        // Test group 1
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)', 1) FROM tbl")
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
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '\\\\$(\\\\d+)', 1) FROM tbl")
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
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)') FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)', 0) FROM tbl")

        // Test complex patterns
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '(\\\\d{3}-\\\\d{3}-\\\\d{4})', 0) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '@([a-z]*)', 1) FROM tbl")

        // Test with multiple groups
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z])(\\\\d*)', 1) FROM tbl")
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
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\\\d+)', 1) FROM tbl")

        // Test with complex patterns
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\\\d+)\\\\.(\\\\d+)\\\\.(\\\\d+)\\\\.(\\\\d+)', 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, 'PORT:(\\\\d+)', 1) FROM tbl")

        // Test with multiple groups - extract second group
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '([a-z])(\\\\d+)', 2) FROM tbl")
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
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '(\\\\d+)', 1) FROM tbl")
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)', 1) FROM tbl")

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
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z])(\\\\d+)', 0) FROM tbl")
        // Test extracting group 1 (letters)
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z])(\\\\d+)', 1) FROM tbl")
        // Test extracting group 2 (digits)
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z])(\\\\d+)', 2) FROM tbl")

        // Test with three groups
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\\\d+)([a-z]+)', 1) FROM tbl")
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\\\d+)([a-z]+)', 2) FROM tbl")
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract_all(_1, '([a-z]+)(\\\\d+)([a-z]+)', 3) FROM tbl")
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
          "SELECT regexp_extract(_1, '(\\\\d{4})-(\\\\d{2})-(\\\\d{2})', 1) FROM tbl")

        // Extract month from date
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\\\d{4})-(\\\\d{2})-(\\\\d{2})', 2) FROM tbl")

        // Extract IP octets
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\\\d+)\\\\.(\\\\d+)\\\\.(\\\\d+)\\\\.(\\\\d+)', 2) FROM tbl")

        // Extract email domain
        checkSparkAnswerAndOperator("SELECT regexp_extract(_1, '@([a-z.]+)', 1) FROM tbl")

        // Extract time components
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, '(\\\\d{2}):(\\\\d{2}):(\\\\d{2})', 1) FROM tbl")

        // Extract RGB values
        checkSparkAnswerAndOperator(
          "SELECT regexp_extract(_1, 'RGB\\\\((\\\\d+),(\\\\d+),(\\\\d+)\\\\)', 2) FROM tbl")

        // Test regexp_extract_all with complex patterns
        checkSparkAnswerAndOperator("SELECT regexp_extract_all(_1, '(\\\\d+)', 1) FROM tbl")
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
            |  regexp_extract(_1, '(\\\\d+)', 1) as first_match,
            |  regexp_extract_all(_1, '(\\\\d+)', 1) as all_matches
            |FROM tbl""".stripMargin)

        // Verify regexp_extract returns first match only while regexp_extract_all returns all
        checkSparkAnswerAndOperator("""SELECT 
            |  _1,
            |  regexp_extract(_1, '(\\\\d+)', 1) as first_digit,
            |  regexp_extract_all(_1, '(\\\\d+)', 1) as all_digits
            |FROM tbl""".stripMargin)

        // Test with multiple groups
        checkSparkAnswerAndOperator("""SELECT
            |  regexp_extract(_1, '([a-z])(\\\\d+)', 1) as first_letter,
            |  regexp_extract_all(_1, '([a-z])(\\\\d+)', 1) as all_letters,
            |  regexp_extract(_1, '([a-z])(\\\\d+)', 2) as first_digit,
            |  regexp_extract_all(_1, '([a-z])(\\\\d+)', 2) as all_digits
            |FROM tbl""".stripMargin)
      }
    }
  }

}
