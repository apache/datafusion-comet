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

import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometConf.COMET_EXEC_STRICT_FLOATING_POINT

sealed trait SupportLevel

/**
 * Comet either supports this feature with full compatibility with Spark, or may have known
 * differences in some specific edge cases that are unlikely to be an issue for most users.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Compatible(notes: Option[String] = None, nativeOptIn: Option[NativeOptIn] = None)
    extends SupportLevel

/**
 * Comet supports this feature but results can be different from Spark.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Incompatible(notes: Option[String] = None) extends SupportLevel

/** Comet does not support this feature */
case class Unsupported(notes: Option[String] = None) extends SupportLevel

/**
 * Describes a faster native implementation that is available for an expression but not currently
 * selected. The default execution path stays Spark-compatible; setting `configKey` to true opts
 * into the native path and its documented differences.
 */
case class NativeOptIn(configKey: String)

object NativeOptIn {

  /** Shared wording for the `[COMET-INFO]` plan hint, so docs and runtime cannot drift. */
  def message(exprName: String, configKey: String): String =
    s"A native implementation of $exprName is available. Set $configKey=true to enable it. " +
      CometConf.COMPAT_GUIDE
}

object SupportLevel {

  /**
   * Returns true if `dt` is, or transitively contains, an instance of any of the given `DataType`
   * classes. Walks `ArrayType` element, `StructType` fields, and `MapType` key/value at every
   * nesting level.
   */
  def containsType(dt: DataType, classes: Class[_ <: DataType]*): Boolean = {
    if (classes.exists(_.isInstance(dt))) {
      true
    } else {
      dt match {
        case ArrayType(elementType, _) => containsType(elementType, classes: _*)
        case StructType(fields) => fields.exists(f => containsType(f.dataType, classes: _*))
        case MapType(keyType, valueType, _) =>
          containsType(keyType, classes: _*) || containsType(valueType, classes: _*)
        case _ => false
      }
    }
  }

  /**
   * Gate for [[CometConf.COMET_EXEC_STRICT_FLOATING_POINT]]: returns the standard incompatibility
   * reason when strict mode is enabled and `dt` contains a float or double (at any nesting
   * level), and `None` otherwise. Callers wrap the reason with `Incompatible` or pass it to
   * `withFallbackReason` as appropriate.
   *
   * `what` describes the operation being gated, e.g. "Sorting on floating-point" or "MapSort on
   * floating-point key", and is interpolated into the returned message.
   */
  def strictFloatingPointReason(dt: DataType, what: String): Option[String] = {
    if (COMET_EXEC_STRICT_FLOATING_POINT.get() &&
      containsType(dt, classOf[FloatType], classOf[DoubleType])) {
      Some(
        s"$what is not 100% compatible with Spark, and Comet is running with " +
          s"${COMET_EXEC_STRICT_FLOATING_POINT.key}=true. ${CometConf.COMPAT_GUIDE}")
    } else {
      None
    }
  }
}
