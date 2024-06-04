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

package org.apache.comet.fuzz

import scala.util.Random

object Utils {

  def randomChoice[T](list: Seq[T], r: Random): T = {
    list(r.nextInt(list.length))
  }

  def randomWeightedChoice[T](valuesWithWeights: Seq[(T, Double)], r: Random): T = {
    val totalWeight = valuesWithWeights.map(_._2).sum
    val randomValue = r.nextDouble() * totalWeight
    var cumulativeWeight = 0.0

    for ((value, weight) <- valuesWithWeights) {
      cumulativeWeight += weight
      if (cumulativeWeight >= randomValue) {
        return value
      }
    }

    // If for some reason the loop doesn't return, return the last value
    valuesWithWeights.last._1
  }

}
