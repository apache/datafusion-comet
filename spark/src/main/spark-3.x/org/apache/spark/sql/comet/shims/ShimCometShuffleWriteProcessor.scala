package org.apache.spark.sql.comet.shims

import org.apache.spark.{Partition, ShuffleDependency, TaskContext}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ShimCometShuffleWriteProcessor extends ShuffleWriteProcessor {
  override def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {
    // Getting rid of the fake partitionId
    val cometRDD = rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[RDD[ColumnarBatch]]
    val rawIter = cometRDD.iterator(partition, context)
    write(rawIter, dep, mapId, partition.index, context)
  }

  def write(
    inputs: Iterator[_],
    dep: ShuffleDependency[_, _, _],
    mapId: Long,
    mapIndex: Int,
    context: TaskContext): MapStatus
}
