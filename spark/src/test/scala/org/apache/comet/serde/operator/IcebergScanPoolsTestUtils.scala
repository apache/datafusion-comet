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

package org.apache.comet.serde.operator

import org.apache.spark.sql.comet.CometIcebergNativeScanExec

import org.apache.comet.serde.OperatorOuterClass.{DeleteFileList, IcebergDeleteFile, IcebergFilePools, IcebergFileScanTask, IcebergLiteral, IcebergPredicate, IcebergPredicateOperator, IcebergScan, IcebergScanCommon, IcebergUnaryPredicate, PartitionData, PartitionValue, ProjectFieldIdList, SparkStructField}

/** Builders and views for tests of how a native Iceberg scan splits its pools. */
object IcebergScanPoolsTestUtils {

  val metadataLocation = "s3://bucket/table/metadata/v1.metadata.json"

  def deleteFilePath(i: Int): String = f"s3://bucket/table/data/part-$i%08d-deletes.parquet"

  def positionDelete(pathIdx: Int, recordCount: Long): IcebergDeleteFile =
    IcebergDeleteFile
      .newBuilder()
      .setFilePathIdx(pathIdx)
      .setContentType("POSITION_DELETES")
      .setFileFormat("PARQUET")
      .setRecordCount(recordCount)
      .build()

  def partitionValue(value: Int): PartitionData =
    PartitionData
      .newBuilder()
      .addValues(
        PartitionValue
          .newBuilder()
          .setFieldId(1000)
          .setLiteral(IcebergLiteral.newBuilder().setIntVal(value)))
      .build()

  def notNull(column: String): IcebergPredicate =
    IcebergPredicate
      .newBuilder()
      .setUnary(
        IcebergUnaryPredicate
          .newBuilder()
          .setColumn(column)
          .setOp(IcebergPredicateOperator.NotNull))
      .build()

  def fileTask(
      dataFile: String,
      deleteSet: Option[Int],
      partitionData: Option[Int],
      residual: Option[Int]): IcebergFileScanTask = {
    val task = IcebergFileScanTask
      .newBuilder()
      .setDataFilePath(dataFile)
      .setLength(1024)
      .setFileSizeInBytes(1024)
    deleteSet.foreach(task.setDeleteFilesIdx)
    partitionData.foreach(task.setPartitionDataIdx)
    residual.foreach(task.setResidualIdx)
    task.build()
  }

  /** Sets the common-block fields the split leaves alone, so tests can check they survive. */
  def withOtherFields(common: IcebergScanCommon.Builder): IcebergScanCommon.Builder =
    common
      .setMetadataLocation(metadataLocation)
      .setScanHashCode(42)
      .setCatalogName("test_cat")
      .setDataFileConcurrencyLimit(4)
      .putCatalogProperties("s3.region", "us-west-2")
      .addRequiredSchema(SparkStructField.newBuilder().setName("id").setNullable(true))
      .addSchemaPool("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
      .addPartitionSpecPool("{\"spec-id\":0,\"fields\":[]}")
      .addPartitionTypePool("{\"type\":\"struct\",\"fields\":[]}")
      .addNameMappingPool("[]")
      .addProjectFieldIdsPool(ProjectFieldIdList.newBuilder().addFieldIds(1))

  /**
   * A scan like a bucketed merge-on-read table read one bucket per partition. Partition b holds
   * four data files, three delete files only it uses, and its own partition value. Every task
   * carries the same residual.
   */
  def bucketedScan(buckets: Int): (IcebergScanCommon, IndexedSeq[IcebergScan]) = {
    val common = IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .addResidualPool(notNull("payload"))
    val partitions = (0 until buckets).map { b =>
      val set = DeleteFileList.newBuilder()
      (0 until 3).foreach { d =>
        val idx = common.getDeleteFilePoolCount
        common.addDeleteFilePathPool(deleteFilePath(b * 3 + d))
        common.addDeleteFilePool(positionDelete(idx, recordCount = 10))
        set.addDeleteFileIndices(idx)
      }
      val setIdx = common.getDeleteFilesPoolCount
      common.addDeleteFilesPool(set)
      val partitionIdx = common.getPartitionDataPoolCount
      common.addPartitionDataPool(partitionValue(b))
      val scan = IcebergScan.newBuilder()
      (0 until 4).foreach { f =>
        scan.addFileScanTasks(
          fileTask(f"data-$b%08d-$f.parquet", Some(setIdx), Some(partitionIdx), Some(0)))
      }
      scan.build()
    }
    (common.build(), partitions)
  }

  /** A scan whose partitions each read one data file and share one unpartitioned delete file. */
  def globalDeleteScan(partitionCount: Int): (IcebergScanCommon, IndexedSeq[IcebergScan]) = {
    val common = IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .addDeleteFilePathPool(deleteFilePath(0))
      .addDeleteFilePool(positionDelete(0, recordCount = 100))
      .addDeleteFilesPool(DeleteFileList.newBuilder().addDeleteFileIndices(0))
      .build()
    val partitions = (0 until partitionCount).map { p =>
      IcebergScan
        .newBuilder()
        .addFileScanTasks(fileTask(f"data-$p%08d.parquet", Some(0), None, None))
        .build()
    }
    (common, partitions)
  }

  /** A partition's own pools in a common block, so tests can read both the same way. */
  def asCommon(pools: IcebergFilePools): IcebergScanCommon =
    IcebergScanCommon
      .newBuilder()
      .addAllDeleteFilePathPool(pools.getDeleteFilePathPoolList)
      .addAllDeleteFilePool(pools.getDeleteFilePoolList)
      .addAllDeleteFilesPool(pools.getDeleteFilesPoolList)
      .addAllPartitionDataPool(pools.getPartitionDataPoolList)
      .addAllResidualPool(pools.getResidualPoolList)
      .build()

  /** The pool entries a scan ships: the common block's, then each partition's own. */
  def storedPools(scan: CometIcebergNativeScanExec): Seq[IcebergScanCommon] =
    IcebergScanCommon.parseFrom(scan.commonData) +:
      scan
        .buildAllPartitionDataForTesting()
        .toSeq
        .map(bytes => asCommon(IcebergScan.parseFrom(bytes).getOwnPools))

  /** The scan each partition's native plan runs, as the injector builds it. */
  def partitionScans(scan: CometIcebergNativeScanExec): Seq[IcebergScan] = {
    val common = IcebergScanCommon.parseFrom(scan.commonData)
    scan.buildAllPartitionDataForTesting().toSeq.map { bytes =>
      IcebergScanPools.forPartition(common, IcebergScan.parseFrom(bytes))
    }
  }
}
