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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.transactions.TransactionUtils
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.TransactionalExec

/** Spark 4.2 transaction-aware view of Comet's driver-side Iceberg commit node. */
private[org] final class IcebergTransactionalCommitExec private (
    delegate: IcebergCommitExec,
    override val transaction: Option[Transaction])
    extends IcebergCommitExec(
      delegate.batchWrite,
      delegate.write,
      delegate.refreshCache,
      delegate.child,
      delegate.command)
    with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): SparkPlan = copyWith(transaction = txn)

  // TreeNode.transformDown uses fastEquals to decide whether a rule changed a node. The inherited
  // IcebergCommitExec case-class equality does not see transaction, so Spark would otherwise drop
  // the copy returned by withTransaction and retain transaction = None.
  override def canEqual(other: Any): Boolean = other.isInstanceOf[IcebergTransactionalCommitExec]

  override def equals(other: Any): Boolean = other match {
    case that: IcebergTransactionalCommitExec =>
      (this eq that) ||
      (that.canEqual(this) && super.equals(that) && transaction == that.transaction)
    case _ => false
  }

  override def hashCode(): Int = 31 * super.hashCode() + transaction.hashCode()

  // IcebergCommitExec is a case class, so its inherited Product shape does not describe this
  // subclass constructor. Spark clones physical plans between planning and execution; override
  // both copy paths so the attached transaction and child survive those copies.
  override def clone(): IcebergTransactionalCommitExec = {
    val cloned = copyWith(child = child.clone())
    cloned.copyTagsFrom(this)
    cloned
  }

  override def makeCopy(newArgs: Array[AnyRef]): IcebergTransactionalCommitExec = {
    require(newArgs.length == 5, s"expected 5 IcebergCommitExec arguments, got ${newArgs.length}")
    val copied = new IcebergTransactionalCommitExec(
      IcebergCommitExec(
        newArgs(0).asInstanceOf[org.apache.spark.sql.connector.write.BatchWrite],
        newArgs(1).asInstanceOf[org.apache.spark.sql.connector.write.Write],
        newArgs(2).asInstanceOf[IcebergCommitExec.RefreshCache],
        newArgs(3).asInstanceOf[SparkPlan],
        newArgs(4).asInstanceOf[Option[org.apache.comet.iceberg.DeltaCommand]]),
      transaction)
    copied.copyTagsFrom(this)
    copied
  }

  override protected def run(): Seq[InternalRow] =
    runWithHooks(() => transaction.foreach(TransactionUtils.commit))

  override protected def withNewChildInternal(
      newChild: SparkPlan): IcebergTransactionalCommitExec =
    copyWith(child = newChild)

  private def copyWith(
      child: SparkPlan = this.child,
      transaction: Option[Transaction] = this.transaction): IcebergTransactionalCommitExec =
    new IcebergTransactionalCommitExec(
      IcebergCommitExec(batchWrite, write, refreshCache, child, command),
      transaction)
}

private[org] object IcebergTransactionalCommitExec {
  def apply(commit: IcebergCommitExec): SparkPlan =
    new IcebergTransactionalCommitExec(commit, None)
}
