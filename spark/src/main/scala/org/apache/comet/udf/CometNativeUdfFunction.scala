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

import org.apache.spark.sql.types.DataType

/** What a native UDF was registered with. */
case class NativeUdfMetadata(
    libraryPath: String,
    inputTypes: Seq[DataType],
    returnType: DataType,
    deterministic: Boolean)

/**
 * The function held by every `ScalaUDF` a native UDF's calls resolve to, carrying the UDF's
 * registration.
 *
 * `CometScalaUDF` recognizes a native UDF by this function rather than by its name. An ordinary
 * UDF registered later under the same name, in this session or another, holds a function of its
 * own and so is never answered out of the native library. Carrying the registration here also
 * means planning needs no registry beyond the session's own function registry.
 *
 * Comet replaces the call with a `NativeScalarUdf` before execution, so Spark never runs this. If
 * it does, which means Comet did not take the operator hosting the call, it throws.
 *
 * `ScalaUDF` casts its function to the `FunctionN` of its arity, hence one subclass per arity.
 */
sealed abstract class CometNativeUdfFunction(val name: String, val meta: NativeUdfMetadata)
    extends Serializable {
  protected def notEvaluated(): Nothing = throw new CometNativeUdfNotEvaluatedException(name)
}

object CometNativeUdfFunction {

  /** The most arguments a native UDF can take. */
  val MaxArity = 4

  def apply(name: String, meta: NativeUdfMetadata): CometNativeUdfFunction =
    meta.inputTypes.length match {
      case 0 => new Arity0(name, meta)
      case 1 => new Arity1(name, meta)
      case 2 => new Arity2(name, meta)
      case 3 => new Arity3(name, meta)
      case 4 => new Arity4(name, meta)
      case n =>
        throw new IllegalArgumentException(
          s"native UDF '$name' takes $n arguments, but at most $MaxArity are supported. See " +
            "https://github.com/apache/datafusion-comet/issues/6177")
    }

  private final class Arity0(name: String, meta: NativeUdfMetadata)
      extends CometNativeUdfFunction(name, meta)
      with (() => Any) {
    override def apply(): Any = notEvaluated()
  }

  private final class Arity1(name: String, meta: NativeUdfMetadata)
      extends CometNativeUdfFunction(name, meta)
      with (Any => Any) {
    override def apply(a: Any): Any = notEvaluated()
  }

  private final class Arity2(name: String, meta: NativeUdfMetadata)
      extends CometNativeUdfFunction(name, meta)
      with ((Any, Any) => Any) {
    override def apply(a: Any, b: Any): Any = notEvaluated()
  }

  private final class Arity3(name: String, meta: NativeUdfMetadata)
      extends CometNativeUdfFunction(name, meta)
      with ((Any, Any, Any) => Any) {
    override def apply(a: Any, b: Any, c: Any): Any = notEvaluated()
  }

  private final class Arity4(name: String, meta: NativeUdfMetadata)
      extends CometNativeUdfFunction(name, meta)
      with ((Any, Any, Any, Any) => Any) {
    override def apply(a: Any, b: Any, c: Any, d: Any): Any = notEvaluated()
  }
}
