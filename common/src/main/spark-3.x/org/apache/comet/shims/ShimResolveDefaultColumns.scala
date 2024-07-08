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

package org.apache.comet.shims

import scala.util.Try

import org.apache.spark.sql.types.{StructField, StructType}

object ShimResolveDefaultColumns {
  // TODO: remove after dropping Spark 3.3 support and directly use ResolveDefaultColumns
  def getExistenceDefaultValue(field: StructField): Any =
    Try {
      // scalastyle:off classforname
      Class.forName("org.apache.spark.sql.catalyst.util.ResolveDefaultColumns$")
      // scalastyle:on classforname
    }.map { objClass =>
      val objInstance = objClass.getField("MODULE$").get(null)
      val method = objClass.getMethod("getExistenceDefaultValues", classOf[StructType])
      method.invoke(objInstance, StructType(Seq(field))).asInstanceOf[Array[Any]].head
    }.getOrElse(null)
}
