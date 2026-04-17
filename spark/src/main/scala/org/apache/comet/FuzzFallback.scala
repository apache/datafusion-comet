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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.execution.SparkPlan

/**
 * Diagnostic utility that randomly vetoes Comet conversions so the rule pipeline produces
 * irregular Spark/Comet boundaries. Used by fuzz tests to surface plan-shape bugs that arise when
 * adjacent operators belong to different execution modes (e.g. the assertion failure described in
 * issue #3949).
 *
 * Determinism: each decision is a pure function of the seed, the node identity hash, and the
 * decision kind. This means repeated calls for the same node return the same answer (important
 * because `getSupportLevel` and `createExec` are called at different times during rule
 * application), and a failing seed can be reproduced by rerunning the test with the same
 * configuration.
 */
object FuzzFallback {

  // Cache decisions per (kind, identityHashCode(plan)). The cache is cleared between queries via
  // reset(); identity hash collisions within one query are astronomically unlikely.
  private val decisions = new ConcurrentHashMap[(Int, Int), Boolean]()

  /** Reset cached decisions. Call this between queries so every query starts clean. */
  def reset(): Unit = decisions.clear()

  private def decide(kind: Int, plan: SparkPlan, probability: Double): Boolean = {
    if (probability <= 0.0) return false
    val key = (kind, System.identityHashCode(plan))
    val cached = decisions.get(key)
    if (cached != null) return cached
    val seed = CometConf.COMET_FUZZ_FALLBACK_SEED.get()
    // Mix seed, kind, and node identity into a deterministic hash, then compare against the
    // probability. Using SplitMix64-style avalanche gives a reasonable uniform distribution.
    var h: Long = seed
    h ^= kind.toLong * 0x9e3779b97f4a7c15L
    h ^= System.identityHashCode(plan).toLong * 0xbf58476d1ce4e5b9L
    h ^= h >>> 30
    h *= 0xbf58476d1ce4e5b9L
    h ^= h >>> 27
    h *= 0x94d049bb133111ebL
    h ^= h >>> 31
    // Map to [0.0, 1.0)
    val u = (h >>> 11) * (1.0 / (1L << 53))
    val result = u < probability
    decisions.put(key, result)
    result
  }

  /**
   * Decide whether to veto converting this shuffle exchange to a Comet shuffle. Returns false
   * unless fuzz fallback is enabled. When enabled, returns true with probability
   * `spark.comet.fuzz.fallback.shuffleVetoProbability`.
   */
  def shouldVetoShuffle(plan: SparkPlan): Boolean = {
    if (!CometConf.COMET_FUZZ_FALLBACK_ENABLED.get()) false
    else decide(1, plan, CometConf.COMET_FUZZ_FALLBACK_SHUFFLE_VETO_PROBABILITY.get())
  }

  /**
   * Decide whether to veto converting this operator to a Comet equivalent. Returns false unless
   * fuzz fallback is enabled. When enabled, returns true with probability
   * `spark.comet.fuzz.fallback.execVetoProbability`.
   */
  def shouldVetoExec(plan: SparkPlan): Boolean = {
    if (!CometConf.COMET_FUZZ_FALLBACK_ENABLED.get()) false
    else decide(2, plan, CometConf.COMET_FUZZ_FALLBACK_EXEC_VETO_PROBABILITY.get())
  }
}
