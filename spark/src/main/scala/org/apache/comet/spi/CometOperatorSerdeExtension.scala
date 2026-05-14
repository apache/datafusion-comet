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
  def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]]
}
