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

package org.apache.comet.spi

import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.CometOperatorSerde

/**
 * SPI hook that lets a contrib extension contribute additional operator-to-native serdes to
 * `CometExecRule`. Used when a contrib needs to translate a contrib-specific physical operator
 * (e.g. `CometDeltaNativeScanExec` for Delta) into a native plan -- the contrib provides the
 * serde, and `CometExecRule` calls it during plan transformation.
 *
 * `CometExecRule` discovers implementations via `CometExtensionRegistry.serdeExtensions`
 * (ServiceLoader-backed). Each contrib JAR ships a
 * `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension` resource listing its
 * extension class.
 *
 * Implementations MUST be stateless / safe to share across query executions.
 */
trait CometOperatorSerdeExtension {

  /** Human-readable name shown in logs and error messages. */
  def name: String

  /**
   * Mapping of SparkPlan class -> serde. The contrib lists every operator class it knows how to
   * translate to native. `CometExecRule` merges these mappings with its built-in `allExecs` to
   * dispatch by class identity at conversion time.
   *
   * Convention: each contrib's mapping should reference only classes the contrib itself defines,
   * so two contribs never claim ownership of the same operator class.
   */
  def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = Map.empty

  /**
   * Predicate-based dispatch hook for contribs whose serde key cannot be expressed as a unique
   * `SparkPlan` class. The canonical case is the `CometScanExec` marker-with-`scanImpl`-tag
   * pattern: a contrib's `CometScanRuleExtension.transformV1` returns `CometScanExec(scanExec,
   * session, "my-contrib-tag")`, but `CometScanExec` is a case class shared with core, so a
   * class-keyed map can't disambiguate by the tag. The contrib overrides this method to inspect
   * the plan and return its serde:
   *
   * {{{
   *   private val MyScanImpl = "native_myformat_compat"   // contrib-local constant
   *
   *   override def matchOperator(op: SparkPlan): Option[CometOperatorSerde[_]] = op match {
   *     case s: CometScanExec if s.scanImpl == MyScanImpl => Some(CometMyFormatScan)
   *     case _ => None
   *   }
   * }}}
   *
   * `CometExecRule` consults `matchOperator` only after the class-keyed `serdes` map misses, so
   * contribs with a unique exec class never need to implement this. Multiple registered
   * extensions' `matchOperator` returns are tried in registration order; the first `Some` wins.
   */
  def matchOperator(op: SparkPlan): Option[CometOperatorSerde[_]] = None

  /**
   * Declares which `scanImpl` string tags this contrib produces from
   * `CometScanRuleExtension.transformV1` when using the `CometScanExec(marker, scanImpl=X)`
   * pattern. Tags listed here get `CometScanExec.supportedDataFilters`'s native-parquet filter
   * exclusions (drop dynamic pruning + IsNull/IsNotNull on ArrayType columns), the same treatment
   * `SCAN_NATIVE_DATAFUSION` receives.
   *
   * Override only if your contrib uses the marker-class pattern AND your native side goes through
   * Comet's tuned `ParquetSource`. Contribs that define their own `SparkPlan` subclass (rather
   * than reusing `CometScanExec`) don't need this; they control filter selection themselves.
   *
   * Example: a Delta contrib that uses `CometScanExec(..., scanImpl="native_delta_compat")` would
   * override this to `Set("native_delta_compat")`.
   */
  def nativeParquetScanImpls: Set[String] = Set.empty

  /**
   * One-shot initialization hook invoked exactly once per JVM by `CometExtensionRegistry.load`
   * after this extension has been instantiated. Use to register executor-side callbacks that
   * can't be expressed declaratively in the `serdes` map -- e.g. a per-partition metadata
   * handler on `CometExecRDD.registerPartitionMetadataHandler` for populating Spark
   * thread-locals from a contrib's serialized per-partition payload.
   *
   * Default no-op so existing extensions don't have to opt in. Implementations MUST be safe
   * to call once per JVM (e.g. don't accumulate state across query executions). Failures are
   * logged and isolated: a broken `init` on one contrib doesn't take down the others.
   */
  def init(): Unit = ()
}
