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
    val byName = entries.map(e => e.name -> e).toMap

    val append = byName("array_append")
    assert(append.group == "array_funcs")
    assert(append.className.endsWith("ArrayAppend"))

    // symbolic operator entries are present too
    assert(byName.contains("+"))
    assert(byName("+").group == "math_funcs")
  }
}
