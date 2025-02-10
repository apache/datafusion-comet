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

package org.apache.spark.sql.comet.execution.shuffle

import scala.reflect.ClassTag

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

/**
 * A [[ShuffleDependency]] that allows us to identify the shuffle dependency as a Comet shuffle.
 */
class CometShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    override val partitioner: Partitioner,
    override val serializer: Serializer = SparkEnv.get.serializer,
    override val keyOrdering: Option[Ordering[K]] = None,
    override val aggregator: Option[Aggregator[K, V, C]] = None,
    override val mapSideCombine: Boolean = false,
    override val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor,
    val shuffleType: ShuffleType = CometNativeShuffle,
    val schema: Option[StructType] = None,
    val decodeTime: SQLMetric)
    extends ShuffleDependency[K, V, C](
      _rdd,
      partitioner,
      serializer,
      keyOrdering,
      aggregator,
      mapSideCombine,
      shuffleWriterProcessor) {}

/** Indicates shuffle type */
sealed trait ShuffleType

/** Indicates that the shuffle is performed by Comet native library */
case object CometNativeShuffle extends ShuffleType

/** Indicates that the shuffle is performed by Comet JVM class */
case object CometColumnarShuffle extends ShuffleType
