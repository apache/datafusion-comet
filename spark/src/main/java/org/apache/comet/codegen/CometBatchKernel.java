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

package org.apache.comet.codegen;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Abstract base extended by the Janino-compiled batch kernel emitted by {@code
 * CometBatchKernelCodegen}. The generated subclass extends {@code CometInternalRow} (so Spark's
 * {@code BoundReference.genCode} can call {@code this.getUTF8String(ord)} directly) and carries
 * typed input fields baked at codegen time, one per input column. Expression evaluation plus Arrow
 * read/write fuse into one method per expression tree.
 */
public abstract class CometBatchKernel extends CometInternalRow {

  protected final Object[] references;

  protected CometBatchKernel(Object[] references) {
    this.references = references;
  }

  /**
   * Run partition-dependent initialization. The generated subclass overrides this to execute
   * statements collected via {@code CodegenContext.addPartitionInitializationStatement}, e.g.
   * reseeding {@code Rand}'s {@code XORShiftRandom} from {@code seed + partitionIndex}.
   * Deterministic expressions leave this as a no-op.
   *
   * <p>The caller invokes this before the first {@code process} call of each partition. The
   * generated subclass is not thread-safe across concurrent {@code process} calls. The dispatcher
   * allocates one per partition and serializes calls.
   */
  public void init(int partitionIndex) {}

  /**
   * Process one batch.
   *
   * @param inputs Arrow input vectors. Length and concrete classes match the schema the kernel was
   *     compiled against.
   * @param output Arrow output vector. Caller allocates to the expression's {@code dataType}.
   * @param numRows number of rows in this batch
   */
  public abstract void process(ValueVector[] inputs, FieldVector output, int numRows);
}
