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
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StringType}

import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Unsupported}

/**
 * https://github.com/apache/datafusion-comet/issues/4489
 *
 * `CometCast.isSupported` matches string casts via `case (DataTypes.StringType, _)` and `case (_,
 * DataTypes.StringType)`, where `DataTypes.StringType` is the singleton default-collation
 * instance. Scala pattern equality means a non-default-collation `StringType` (e.g. `STRING
 * COLLATE UTF8_LCASE`) does not match either case and falls through to the `unsupported(...)`
 * catch-all (directly, or via `canCastFromString`/`canCastToString`'s own catch-all when only one
 * side is collated), so the cast already falls back to Spark correctly. That was previously
 * implicit and untested; this suite pins it down directly against `isSupported` rather than
 * relying on Scala's pattern-match semantics never changing.
 *
 * This lives under `spark-4.x` (shared across every 4.x profile) rather than `spark-4.1+`,
 * because collation is a Spark 4.0+ feature (`StringType(collationName)` already resolves on
 * 4.0), unlike `TimeType` in #4490 which is 4.1-only.
 */
class CometCastCollatedStringSuite extends CometTestBase {

  private val lcase = StringType("UTF8_LCASE")
  private val unicode = StringType("UNICODE")

  private def assertUnsupported(fromType: DataType, toType: DataType): Unit = {
    Seq(CometEvalMode.LEGACY, CometEvalMode.TRY, CometEvalMode.ANSI).foreach { evalMode =>
      CometCast.isSupported(fromType, toType, None, evalMode) match {
        case _: Unsupported => // expected
        case other =>
          fail(s"expected Unsupported for $fromType -> $toType under $evalMode, got $other")
      }
    }
  }

  test("cast non-default collated string to IntegerType falls back (Unsupported)") {
    assertUnsupported(lcase, IntegerType)
  }

  test("cast IntegerType to non-default collated string falls back (Unsupported)") {
    assertUnsupported(IntegerType, lcase)
  }

  test("cast non-default collated string to default-collation StringType falls back") {
    assertUnsupported(lcase, DataTypes.StringType)
  }

  test("cast default-collation StringType to non-default collated string falls back") {
    assertUnsupported(DataTypes.StringType, lcase)
  }

  test("cast between two different non-default collations falls back") {
    assertUnsupported(lcase, unicode)
  }

  // Boundary case, not a bug: an identity cast between two instances of the SAME collation is a
  // byte-for-byte no-op that never touches collation-aware comparison/hashing/sorting semantics,
  // so it is correctly Compatible() regardless of which collation it is. This documents that
  // boundary so a future reader does not mistake it for the gap this issue describes.
  test("cast identical non-default collation to itself is a Compatible no-op") {
    Seq(CometEvalMode.LEGACY, CometEvalMode.TRY, CometEvalMode.ANSI).foreach { evalMode =>
      assert(CometCast.isSupported(lcase, lcase, None, evalMode) == Compatible())
    }
  }

  test("cast default-collation StringType to itself is a Compatible no-op (sanity baseline)") {
    Seq(CometEvalMode.LEGACY, CometEvalMode.TRY, CometEvalMode.ANSI).foreach { evalMode =>
      assert(
        CometCast.isSupported(DataTypes.StringType, DataTypes.StringType, None, evalMode) ==
          Compatible())
    }
  }
}
