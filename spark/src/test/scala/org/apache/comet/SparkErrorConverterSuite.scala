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

  test("CannotReadFile converts to a FAILED_READ_FILE SparkException naming the file") {
    val ex = SparkErrorConverter
      .convertErrorType(
        "CannotReadFile",
        "",
        Map(
          "filePath" -> "file:/tmp/data/part-0.parquet",
          "message" -> "Parquet error: bad footer"),
        Array.empty,
        null)
      .getOrElse(fail("Expected CannotReadFile to be converted to a Spark exception"))
    assert(ex.getMessage.contains("FAILED_READ_FILE"))
    assert(ex.getMessage.contains("part-0.parquet"))
  }

  test("CannotReadFile with empty native path falls back to the per-task file list") {
    // The native error (e.g. corrupt parquet) carries no path; convertToSparkException must fill
    // it from the per-task file list threaded in from CometExecIterator.
    val json =
      """{"errorType":"CannotReadFile","errorClass":"",""" +
        """"params":{"filePath":"","message":"Parquet error: bad footer"}}"""
    val ex = SparkErrorConverter.convertToSparkException(
      new org.apache.comet.exceptions.CometQueryExecutionException(json),
      taskFilePaths = Seq("file:/tmp/data/part-7.parquet"))
    assert(ex.getMessage.contains("FAILED_READ_FILE"))
    assert(ex.getMessage.contains("part-7.parquet"))
  }

  test("CannotReadFile prefers the native path over the per-task file list") {
    // When object_store supplied the path (NotFound), keep it rather than the fallback list.
    val json =
      """{"errorType":"CannotReadFile","errorClass":"",""" +
        """"params":{"filePath":"file:/tmp/data/native.parquet","message":"Object at location ... not found"}}"""
    val ex = SparkErrorConverter.convertToSparkException(
      new org.apache.comet.exceptions.CometQueryExecutionException(json),
      taskFilePaths = Seq("file:/tmp/data/fallback.parquet"))
    assert(ex.getMessage.contains("native.parquet"))
    assert(!ex.getMessage.contains("fallback.parquet"))
  }

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
