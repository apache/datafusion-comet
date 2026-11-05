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

package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.collection.BitSet

private[comet] object ExtendedFileSourceScanExec {
  def apply(output: Seq[Attribute]): FileSourceScanExec = {
    new ExtendedFileSourceScanExec(
      null,
      None,
      output,
      new StructType().add("id", IntegerType),
      Nil,
      None,
      None,
      Nil,
      Some(TableIdentifier("test")),
      true,
      true,
      output)
  }
}

// Model a distribution that adds scan fields. An inherited case-class copy would discard them.
private[comet] class ExtendedFileSourceScanExec(
    relation: HadoopFsRelation,
    stream: Option[SparkDataStream],
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean,
    val vendorFlag: Boolean,
    val vendorOutput: Seq[Attribute])
    extends FileSourceScanExec(
      relation,
      stream,
      output,
      requiredSchema,
      partitionFilters,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      dataFilters,
      tableIdentifier,
      disableBucketedScan) {
  override def productArity: Int = 12

  override def productElement(index: Int): Any = index match {
    case 10 => vendorFlag
    case 11 => vendorOutput
    case _ => super.productElement(index)
  }
}
