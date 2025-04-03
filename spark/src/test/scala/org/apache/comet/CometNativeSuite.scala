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

package org.apache.comet

import org.apache.spark.SparkException
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.comet.{CometExec, CometExecUtils}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch

class CometNativeSuite extends CometTestBase {
  test("test handling NPE thrown by JVM") {
    val rdd = spark.range(0, 1).rdd.map { value =>
      val limitOp =
        CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
      val cometIter = CometExec.getCometIterator(
        Seq(new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = true
          override def next(): ColumnarBatch = throw new NullPointerException()
        }),
        1,
        limitOp,
        1,
        0)
      cometIter.next()
      cometIter.close()
      value
    }

    val exception = intercept[SparkException] {
      rdd.collect()
    }
    assert(exception.getMessage contains "java.lang.NullPointerException")
  }

  test("handling NPE when closing null handle of parquet reader") {
    assert(NativeBase.isLoaded)
    val exception1 = intercept[NullPointerException] {
      parquet.Native.closeRecordBatchReader(0)
    }
    assert(exception1.getMessage contains "null batch context handle")

    val exception2 = intercept[NullPointerException] {
      parquet.Native.closeColumnReader(0)
    }
    assert(exception2.getMessage contains "null context handle")
  }
}
