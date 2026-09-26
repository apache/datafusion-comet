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

import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, CreateTableAsSelectExec, ReplaceTableAsSelectExec}

/**
 * Spark 3.4: CTAS and RTAS write the table from the create or replace exec itself, through
 * `TableWriteExecHelper.writeWithV2`, so the write never appears as a write node of its own.
 */
private[iceberg] object IcebergTableAsSelectShim {

  /** The catalog `plan` writes through, when `plan` is a CTAS or RTAS exec. */
  def writeCatalog(plan: SparkPlan): Option[TableCatalog] = plan match {
    case p: CreateTableAsSelectExec => Some(p.catalog)
    case p: AtomicCreateTableAsSelectExec => Some(p.catalog)
    case p: ReplaceTableAsSelectExec => Some(p.catalog)
    case p: AtomicReplaceTableAsSelectExec => Some(p.catalog)
    case _ => None
  }
}
