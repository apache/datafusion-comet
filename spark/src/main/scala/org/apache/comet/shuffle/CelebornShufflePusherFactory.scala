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

package org.apache.comet.shuffle

import org.apache.spark.{SparkConf, TaskContext}

/** Creates task-owned Celeborn pushers using the application's existing Spark configuration. */
object CelebornShufflePusherFactory {

  private val CELEBORN_ENABLED_KEY = "spark.comet.celeborn.enabled"
  private val SHUFFLE_MANAGER_KEY = "spark.shuffle.manager"
  private val SHUFFLE_DATA_IO_KEY = "spark.shuffle.sort.io.plugin.class"
  private val CELEBORN_MASTER_ENDPOINTS_KEY = "spark.celeborn.master.endpoints"

  private val CELEBORN_SHUFFLE_MANAGER =
    "org.apache.spark.shuffle.celeborn.SparkShuffleManager"
  private val COMET_CELEBORN_SHUFFLE_MANAGER =
    "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager"
  private val CELEBORN_SHUFFLE_DATA_IO =
    "org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO"

  private val MAX_STAGE_ATTEMPTS = 1 << 15
  private val MAX_TASK_ATTEMPTS = 1 << 16

  /** Detects resolved Celeborn configuration while honoring an explicit application opt-out. */
  def isEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(CELEBORN_ENABLED_KEY, true) &&
    (conf
      .getOption(SHUFFLE_MANAGER_KEY)
      .exists(manager =>
        manager == CELEBORN_SHUFFLE_MANAGER || manager == COMET_CELEBORN_SHUFFLE_MANAGER) ||
      conf.getOption(SHUFFLE_DATA_IO_KEY).contains(CELEBORN_SHUFFLE_DATA_IO) ||
      conf.getOption(CELEBORN_MASTER_ENDPOINTS_KEY).exists(_.trim.nonEmpty))
  }

  /** Match Celeborn's stage/task attempt packing without depending on its Spark client jar. */
  private[shuffle] def encodeAttemptNumber(stageAttempt: Int, taskAttempt: Int): Int = {
    require(
      stageAttempt >= 0 && stageAttempt < MAX_STAGE_ATTEMPTS,
      s"Celeborn stage attempt must be between 0 and ${MAX_STAGE_ATTEMPTS - 1}: " +
        stageAttempt)
    require(
      taskAttempt >= 0 && taskAttempt < MAX_TASK_ATTEMPTS,
      s"Celeborn task attempt must be between 0 and ${MAX_TASK_ATTEMPTS - 1}: " +
        taskAttempt)

    (stageAttempt << 16) | taskAttempt
  }

  /**
   * Bind an existing Celeborn client to one Spark map-task attempt.
   *
   * The caller supplies the already-resolved Celeborn shuffle ID; it may differ from Spark's
   * shuffle ID after a stage retry. Task metadata is captured here because native callbacks can
   * execute on threads where Spark's thread-local TaskContext is unavailable.
   */
  def create(
      conf: SparkConf,
      client: AnyRef,
      celebornShuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      taskContext: TaskContext): Option[ShufflePartitionPusher] = {
    if (!isEnabled(conf)) {
      None
    } else {
      val mapId = taskContext.partitionId()
      val stageAttempt = taskContext.stageAttemptNumber()
      val taskAttempt = taskContext.attemptNumber()
      val encodedAttempt = encodeAttemptNumber(stageAttempt, taskAttempt)
      Some(
        new CelebornShufflePartitionPusher(
          client,
          celebornShuffleId,
          mapId,
          encodedAttempt,
          numMappers,
          numPartitions))
    }
  }
}
