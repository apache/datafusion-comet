package org.apache.spark.comet

trait ShimCometDriverPlugin {
  // `org.apache.spark.internal.config.EXECUTOR_MEMORY_OVERHEAD_FACTOR` was added since Spark 3.3.0
  private val EXECUTOR_MEMORY_OVERHEAD_FACTOR = "spark.executor.memoryOverheadFactor"
  private val EXECUTOR_MEMORY_OVERHEAD_FACTOR_DEFAULT = 0.1
  // `org.apache.spark.internal.config.EXECUTOR_MIN_MEMORY_OVERHEAD` was added since Spark 4.0.0
  private val EXECUTOR_MIN_MEMORY_OVERHEAD = "spark.executor.minMemoryOverhead"
  private val EXECUTOR_MIN_MEMORY_OVERHEAD_DEFAULT = 384L

  def getMemoryOverheadFactor =
    sc.getConf.getDouble(
      EXECUTOR_MEMORY_OVERHEAD_FACTOR,
      EXECUTOR_MEMORY_OVERHEAD_FACTOR_DEFAULT)
  def getMemoryOverheadMinMib =
    sc.getConf.getLong(EXECUTOR_MIN_MEMORY_OVERHEAD, EXECUTOR_MIN_MEMORY_OVERHEAD_DEFAULT)
}
