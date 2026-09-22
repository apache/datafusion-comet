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

package org.apache.comet.serde

import org.scalatest.funsuite.AnyFunSuite

class SerdeRegistrationSuite extends AnyFunSuite {

  // A version shim only adds serdes for classes the shared map cannot name on every Spark
  // version. A key present on both sides is either a stale duplicate or a silent override of
  // the shared serde; a version that needs a different serde for a shared class should move
  // that class out of the shared map instead of shadowing it here.
  test("version shims register only classes the shared serde maps do not") {
    import QueryPlanSerde._
    val overlaps = Seq(
      "math" -> (baseMathExpressions, sparkVersionSpecificMathExpressions),
      "map" -> (baseMapExpressions, sparkVersionSpecificMapExpressions),
      "string" -> (baseStringExpressions, sparkVersionSpecificStringExpressions),
      "misc" -> (baseMiscExpressions, sparkVersionSpecificMiscExpressions))
      .flatMap { case (group, (base, shim)) =>
        base.keySet.intersect(shim.keySet).map(cls => s"$group: ${cls.getSimpleName}")
      }
    assert(overlaps.isEmpty, s"shim entries shadow shared serdes: ${overlaps.mkString(", ")}")
  }

  // The combined map is built by merging the groups in order, so a class registered in two
  // groups would silently take the later serde. Every group must own its classes alone.
  test("no expression class is registered in more than one serde group") {
    val owners = QueryPlanSerde.serdeGroups
      .flatMap { case (name, group) => group.keys.map(cls => cls -> name) }
      .groupBy(_._1)
      .collect {
        case (cls, entries) if entries.size > 1 =>
          s"${cls.getSimpleName}: ${entries.map(_._2).mkString(", ")}"
      }
    assert(owners.isEmpty, s"classes registered in several groups: ${owners.mkString("; ")}")
  }

  test("every serde group entry reaches the combined map unchanged") {
    for ((_, group) <- QueryPlanSerde.serdeGroups; (cls, serde) <- group) {
      assert(QueryPlanSerde.exprSerdeMap.get(cls).exists(_ eq serde), cls.getSimpleName)
    }
    val total = QueryPlanSerde.serdeGroups.map(_._2.size).sum
    assert(
      QueryPlanSerde.exprSerdeMap.size == total,
      s"${QueryPlanSerde.exprSerdeMap.size} != $total")
  }
}
