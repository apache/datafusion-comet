package org.apache.spark.comet

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.EXECUTOR_MEMORY_OVERHEAD_FACTOR
import org.apache.spark.internal.config.EXECUTOR_MIN_MEMORY_OVERHEAD

trait ShimCometDriverPlugin {
  def getMemoryOverheadFactor(sparkConf: SparkConf): Double = sparkConf.get(
    EXECUTOR_MEMORY_OVERHEAD_FACTOR)

  def getMemoryOverheadMinMib(sparkConf: SparkConf): Long = sparkConf.get(
    EXECUTOR_MIN_MEMORY_OVERHEAD)
}
