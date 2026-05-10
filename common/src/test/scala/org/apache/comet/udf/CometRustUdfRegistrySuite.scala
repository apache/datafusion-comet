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

package org.apache.comet.udf

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{IntegerType, LongType}

class CometRustUdfRegistrySuite extends AnyFunSuite {

  test("register and look up by name") {
    val r = new CometRustUdfRegistry
    val meta = RustUdfMetadata(
      libraryPath = "/path/to/lib.so",
      inputTypes = Seq(LongType),
      returnType = LongType,
      deterministic = true)
    r.register("add_one", meta)
    assert(r.get("add_one").contains(meta))
    assert(r.get("missing").isEmpty)
  }

  test("re-registration replaces the metadata") {
    val r = new CometRustUdfRegistry
    val a = RustUdfMetadata("/a.so", Seq(IntegerType), IntegerType, true)
    val b = RustUdfMetadata("/b.so", Seq(IntegerType), IntegerType, true)
    r.register("f", a)
    r.register("f", b)
    assert(r.get("f").contains(b))
  }

  test("snapshot returns immutable copy") {
    val r = new CometRustUdfRegistry
    r.register("f", RustUdfMetadata("/a.so", Seq(IntegerType), IntegerType, true))
    val snap = r.snapshot
    r.register("g", RustUdfMetadata("/b.so", Seq(IntegerType), IntegerType, true))
    assert(snap.size == 1)
  }
}
