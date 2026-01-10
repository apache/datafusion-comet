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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, hash}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to validate array hashing behavior in Spark. This generates expected hash values for
 * arrays that we can use to validate the Comet native implementation.
 */
class ArrayHashValidationSuite extends QueryTest with SharedSparkSession {

  test("print hash values for int arrays") {
    import testImplicits._

    val df =
      Seq(Seq(1, 2, 3), Seq(1), Seq(), Seq(0), Seq(-1, -2, -3), Seq(Int.MaxValue, Int.MinValue))
        .toDF("arr")

    println("\n=== Hash values for int arrays ===")
    df.select(hash($"arr").as("hash"), $"arr").show(false)
    df.select(hash($"arr").as("hash"), $"arr").collect().foreach { row =>
      println(s"Array: ${row.getAs[Seq[Int]](1)} => Hash: ${row.getAs[Int](0)}")
    }
  }

  test("print hash values for string arrays") {
    import testImplicits._

    val df =
      Seq(Seq("hello", "world"), Seq("a"), Seq(), Seq(""), Seq("foo", "bar", "baz")).toDF("arr")

    println("\n=== Hash values for string arrays ===")
    df.select(hash($"arr").as("hash"), $"arr").show(false)
    df.select(hash($"arr").as("hash"), $"arr").collect().foreach { row =>
      println(s"Array: ${row.getAs[Seq[String]](1)} => Hash: ${row.getAs[Int](0)}")
    }
  }

  test("print hash values for long arrays") {
    import testImplicits._

    val df = Seq(Seq(1L, 2L, 3L), Seq(Long.MaxValue, Long.MinValue), Seq(0L)).toDF("arr")

    println("\n=== Hash values for long arrays ===")
    df.select(hash($"arr").as("hash"), $"arr").show(false)
    df.select(hash($"arr").as("hash"), $"arr").collect().foreach { row =>
      println(s"Array: ${row.getAs[Seq[Long]](1)} => Hash: ${row.getAs[Int](0)}")
    }
  }

  test("print hash values for arrays with nulls") {
    import testImplicits._

    val df = Seq(Seq(Some(1), None, Some(3)), Seq(None, None), Seq(Some(1))).toDF("arr")

    println("\n=== Hash values for int arrays with nulls ===")
    df.select(hash($"arr").as("hash"), $"arr").show(false)
    df.select(hash($"arr").as("hash"), $"arr").collect().foreach { row =>
      println(s"Array: ${row.getAs[Seq[Option[Int]]](1)} => Hash: ${row.getAs[Int](0)}")
    }
  }

  test("print hash values for nested arrays") {
    import testImplicits._

    val df = Seq(Seq(Seq(1, 2), Seq(3, 4)), Seq(Seq(1)), Seq(Seq())).toDF("arr")

    println("\n=== Hash values for nested int arrays ===")
    df.select(hash($"arr").as("hash"), $"arr").show(false)
    df.select(hash($"arr").as("hash"), $"arr").collect().foreach { row =>
      println(s"Array: ${row.getAs[Seq[Seq[Int]]](1)} => Hash: ${row.getAs[Int](0)}")
    }
  }

  test("compare individual element hashes vs array hash") {
    import testImplicits._

    // This helps us understand how array hashing combines element hashes
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")

    println("\n=== Individual element hashes ===")
    df.select(
      hash($"a").as("hash_a"),
      hash($"b").as("hash_b"),
      hash($"c").as("hash_c"),
      hash(array($"a", $"b", $"c")).as("hash_array"))
      .show(false)
  }

  test("empty vs null array") {
    import testImplicits._

    // Test difference between null array and empty array
    val df = Seq((Seq[Int](), "empty"), (null, "null")).toDF("arr", "label")

    println("\n=== Empty vs Null array hash ===")
    df.select($"label", hash($"arr").as("hash")).show(false)
  }
}
