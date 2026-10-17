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

package org.apache.comet.iceberg

import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergTransactionalCommitExec}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.datasources.v2.TransactionalExec

object IcebergTransactionalCommitTestShim {

  def verifyTransactionAttachment(): Unit = {
    val transaction = new Transaction {
      override def catalog(): CatalogPlugin = null
      override def commit(): Unit = ()
      override def abort(): Unit = ()
      override def registerScans(scans: Array[Scan]): Boolean = false
      override def close(): Unit = ()
    }

    val commit = IcebergCommitExec(
      null.asInstanceOf[BatchWrite],
      null.asInstanceOf[Write],
      () => (),
      LocalTableScanExec(Nil, Nil, None),
      command = Some(DeltaUpdate))
    val original = IcebergCommitPlanShim
      .wrap(commit)
      .asInstanceOf[IcebergTransactionalCommitExec]

    assert(original.transaction.isEmpty)

    val transformed = original.transformDown { case exec: TransactionalExec =>
      exec.withTransaction(Some(transaction))
    }

    assert(!(transformed eq original))
    assert(!original.fastEquals(transformed))

    val attached = transformed.asInstanceOf[IcebergTransactionalCommitExec]
    assert(attached.transaction.contains(transaction))
    assert(attached.command.contains(DeltaUpdate))
    val cloned = attached.clone()
    assert(cloned.transaction.contains(transaction))
    assert(cloned.command.contains(DeltaUpdate))
  }
}
