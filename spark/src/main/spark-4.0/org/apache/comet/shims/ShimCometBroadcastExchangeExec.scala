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

import org.apache.comet.shims.ShimCometBroadcastExchangeExec.SPARK_MAX_BROADCAST_TABLE_SIZE
import org.apache.spark.SparkContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf

trait ShimCometBroadcastExchangeExec {

  def setJobGroupOrTag(sc: SparkContext, broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job tag here so later it may get cancelled by tag if necessary.
    sc.addJobTag(broadcastExchange.jobTag)
    sc.setInterruptOnCancel(true)
  }

  def cancelJobGroup(sc: SparkContext,  broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobsWithTag(broadcastExchange.jobTag)
  }

  def maxBroadcastTableBytes(conf: SQLConf): Long = {
    JavaUtils.byteStringAsBytes(conf.getConfString(SPARK_MAX_BROADCAST_TABLE_SIZE, "8GB"))
  }

}

object ShimCometBroadcastExchangeExec {
  val SPARK_MAX_BROADCAST_TABLE_SIZE = "spark.sql.maxBroadcastTableSize"
}
