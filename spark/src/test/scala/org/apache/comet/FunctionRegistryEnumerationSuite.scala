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

class FunctionRegistryEnumerationSuite extends AnyFunSuite {

  test("enumerates known builtins with group and class") {
    val entries = ExpressionReference.builtinFunctions()
    // Sanity: Spark registers hundreds of builtins; a near-empty result means a
    // classpath/registry problem, so fail clearly rather than via a missing-key lookup.
    assert(entries.size > 100)

    val byName = entries.map(e => e.name -> e).toMap

    val append = byName("array_append")
    assert(append.group == "array_funcs")
    // The concrete backing class varies across Spark versions (e.g. array_append is
    // RuntimeReplaceable in Spark 4.0), so only assert the class name is populated.
    assert(append.className.nonEmpty)

    // symbolic operator entries are present too
    assert(byName.contains("+"))
    assert(byName("+").group == "math_funcs")
  }
}
