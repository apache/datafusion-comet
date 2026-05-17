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

class SparkErrorConverterSuite extends AnyFunSuite {
  private def castOverflowError(fromType: String, value: String): Throwable = {
    SparkErrorConverter
      .convertErrorType(
        "CastOverFlow",
        "CAST_OVERFLOW",
        Map("fromType" -> fromType, "toType" -> "INT", "value" -> value),
        Array.empty,
        null)
      .getOrElse(fail("Expected CastOverFlow to be converted to a Spark exception"))
  }

  private def assertCastOverflowContains(
      fromType: String,
      value: String,
      expectedMessagePart: String): Unit = {
    val err = castOverflowError(fromType, value)
    assert(
      !err.isInstanceOf[NumberFormatException],
      s"Unexpected parse failure for $fromType $value")
    assert(
      err.getMessage.contains(expectedMessagePart),
      s"Expected '${err.getMessage}' to contain '$expectedMessagePart' for $fromType $value")
  }

  private def assertCastOverflowContainsNaN(fromType: String, value: String): Unit = {
    val err = castOverflowError(fromType, value)
    assert(
      !err.isInstanceOf[NumberFormatException],
      s"Unexpected parse failure for $fromType $value")
    assert(
      err.getMessage.toLowerCase.contains("nan"),
      s"Expected '${err.getMessage}' to contain NaN for $fromType $value")
  }

  test("CastOverFlow conversion handles all float positive infinity literals") {
    Seq("inf", "+inf", "infinity", "+infinity").foreach { value =>
      assertCastOverflowContains("FLOAT", value, "Infinity")
    }
  }

  test("CastOverFlow conversion handles all float negative infinity literals") {
    Seq("-inf", "-infinity").foreach { value =>
      assertCastOverflowContains("FLOAT", value, "-Infinity")
    }
  }

  test("CastOverFlow conversion handles all float NaN literals") {
    Seq("nan", "+nan", "-nan").foreach { value =>
      assertCastOverflowContainsNaN("FLOAT", value)
    }
  }

  test("CastOverFlow conversion handles float standard numeric literal fallback") {
    assertCastOverflowContains("FLOAT", "1.5", "1.5")
  }

  test("CastOverFlow conversion handles all double positive infinity literals") {
    Seq("inf", "infd", "+inf", "+infd", "infinity", "infinityd", "+infinity", "+infinityd")
      .foreach { value =>
        assertCastOverflowContains("DOUBLE", value, "Infinity")
      }
  }

  test("CastOverFlow conversion handles all double negative infinity literals") {
    Seq("-inf", "-infd", "-infinity", "-infinityd").foreach { value =>
      assertCastOverflowContains("DOUBLE", value, "-Infinity")
    }
  }

  test("CastOverFlow conversion handles all double NaN literals") {
    Seq("nan", "nand", "+nan", "+nand", "-nan", "-nand").foreach { value =>
      assertCastOverflowContainsNaN("DOUBLE", value)
    }
  }

  test("CastOverFlow conversion handles double standard numeric literal fallback") {
    assertCastOverflowContains("DOUBLE", "1.5", "1.5")
    assertCastOverflowContains("DOUBLE", "1.5d", "1.5")
  }
}
