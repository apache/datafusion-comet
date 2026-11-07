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

package org.apache.spark.sql.hive

import java.util.Arrays

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper

/** Registers Hive UDTFs in a session that has no Hive support, as `HiveSessionCatalog` would. */
object CometHiveUDTFs {
  def register(spark: SparkSession, name: String, udtfClass: Class[_ <: GenericUDTF]): Unit =
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier(name),
      new ExpressionInfo(udtfClass.getName, name),
      (children: Seq[Expression]) =>
        HiveGenericUDTF(name, new HiveFunctionWrapper(udtfClass.getName), children))
}

/**
 * Emits each input value with a running count of the rows it has processed, and from `close` one
 * more row holding the total. The count is per instance, so it shows whether the rows of a
 * partition reach one instance, and the `close` row whether `terminate` runs.
 */
class CountingUDTF extends GenericUDTF {
  private var count = 0L

  override def initialize(argOIs: StructObjectInspector): StructObjectInspector =
    ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("value", "n"),
      Arrays.asList[ObjectInspector](
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaLongObjectInspector))

  override def process(args: Array[AnyRef]): Unit = {
    count += 1
    forward(Array[AnyRef](Option(args(0)).map(_.toString).orNull, Long.box(count)))
  }

  override def close(): Unit = forward(Array[AnyRef]("total", Long.box(count)))
}
