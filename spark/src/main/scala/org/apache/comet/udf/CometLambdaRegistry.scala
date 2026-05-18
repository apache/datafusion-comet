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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Thread-safe registry bridging plan-time Spark expressions to execution-time UDF lookup. At plan
 * time the serde layer registers a lambda expression under a unique key; at execution time the
 * UDF retrieves it by that key (passed as a scalar argument).
 */
object CometLambdaRegistry {

  private val registry = new ConcurrentHashMap[String, Expression]()

  def register(expression: Expression): String = {
    val key = UUID.randomUUID().toString
    registry.put(key, expression)
    key
  }

  def get(key: String): Expression = {
    val expr = registry.get(key)
    if (expr == null) {
      throw new IllegalStateException(
        s"Lambda expression not found in registry for key: $key. " +
          "This indicates a lifecycle issue between plan creation and execution.")
    }
    expr
  }

  def remove(key: String): Unit = {
    registry.remove(key)
  }

  // Visible for testing
  def size(): Int = registry.size()
}
