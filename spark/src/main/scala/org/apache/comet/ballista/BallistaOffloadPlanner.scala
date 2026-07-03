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

package org.apache.comet.ballista

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.comet.{CometExec, CometNativeExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan

import com.google.protobuf.ByteString

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.{OffloadFragment, OffloadInput}
import org.apache.comet.serde.OperatorOuterClass.CometBallistaOffloadPlan

/**
 * Driver-side DAG walker that decomposes a Comet physical plan into a fragment/hash-exchange DAG
 * and serializes it as a `CometBallistaOffloadPlan` protobuf for submission to Ballista via
 * `NativeBallista.executeOffloadPlan`.
 *
 * Currently supports:
 *   - a single native block (no Comet exchange): one fragment, no inputs.
 *   - an N-block LINEAR chain of native blocks connected by [[CometShuffleExchangeExec]] hash
 *     exchanges (the R2 two-stage GROUP BY shape generalized to N stages).
 *
 * General join / multi-input DAG shapes (a native block fed by more than one upstream fragment)
 * are a future increment; the walker rejects anything it doesn't recognize with an
 * [[UnsupportedOperationException]] rather than guessing.
 */
object BallistaOffloadPlanner {

  /** A native block plus the exchanges that directly feed it (its DAG inputs). */
  private case class BlockNode(block: CometNativeExec, inputs: Seq[CometShuffleExchangeExec])

  /**
   * Decompose `root` into a topologically-ordered DAG of native blocks + hash exchanges and
   * serialize it as a CometBallistaOffloadPlan. Producers precede consumers; the last fragment is
   * the root. Throws [[UnsupportedOperationException]] for shapes not yet supported.
   */
  def buildOffloadPlan(root: SparkPlan, numPartitions: Int): Array[Byte] = {
    // Assign a fragment index to every native block, discovered in producer-first order.
    val ordered = mutable.ArrayBuffer.empty[BlockNode]
    val indexOf = mutable.LinkedHashMap.empty[CometNativeExec, Int]

    // `p` itself is usually the native block, but the very top of the collect root may be a
    // thin wrapper with no serialized plan of its own (e.g. `CometNativeColumnarToRowExec`, the
    // columnar-to-row conversion node that carries the `executeCollect` override which calls in
    // here). `collectFirst` is a pre-order search, so it finds the nearest enclosing boundary
    // (the outermost `CometNativeExec` with a serialized plan) without ever having to look past
    // it into that block's own internals.
    def blockOf(p: SparkPlan): CometNativeExec =
      p.collectFirst { case n: CometNativeExec if n.serializedPlanOpt.isDefined => n }
        .getOrElse(
          throw new UnsupportedOperationException(
            "Comet Ballista offload: expected a serialized native block reachable from " +
              s"${p.nodeName}:\n$root"))

    // The direct native-block inputs of `block` are the Comet exchanges in its subtree that are
    // not nested under a deeper native block.
    def directExchanges(block: CometNativeExec): Seq[CometShuffleExchangeExec] = {
      val found = mutable.ArrayBuffer.empty[CometShuffleExchangeExec]
      def walk(p: SparkPlan): Unit = p match {
        case e: CometShuffleExchangeExec => found += e // do NOT descend past an exchange
        case other => other.children.foreach(walk)
      }
      block.children.foreach(walk)
      found.toSeq
    }

    def register(block: CometNativeExec): Int = indexOf.getOrElseUpdate(
      block, {
        val inputs = directExchanges(block)
        // Linear-chain guard (Task 5 scope): a native block may have at most one upstream
        // fragment. A block fed by more than one hash exchange is a join/multi-input DAG shape,
        // out of scope until Task 6.
        if (inputs.size > 1) {
          throw new UnsupportedOperationException(
            "Comet Ballista offload: block is fed by more than one Comet exchange " +
              s"(${inputs.size}); multi-input DAG shapes (e.g. joins) are not yet supported:\n" +
              s"$root")
        }
        // Recurse producers first so their indices are smaller (topological order).
        inputs.foreach(ex => register(blockOf(ex.child)))
        val idx = ordered.size
        ordered += BlockNode(block, inputs)
        indexOf.put(block, idx)
        idx
      })

    register(blockOf(root))

    // A multi-fragment plan feeds downstream fragments from the Ballista hash shuffle, which
    // requires each consuming fragment's shuffle-input leaf to serialize as a plain `Scan`
    // (#100), not a native `ShuffleScan` (#116) that expects to read Comet shuffle blocks
    // directly. That requires direct read disabled (mirrors the old two-block R2 check).
    if (ordered.size > 1 && CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.get()) {
      throw new UnsupportedOperationException(
        "Comet Ballista multi-fragment offload requires " +
          s"${CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key}=false so each downstream " +
          "fragment reads a plain Scan leaf (fed by the Ballista shuffle) rather than a " +
          s"native ShuffleScan:\n$root")
    }

    val planBuilder = CometBallistaOffloadPlan.newBuilder().setNumPartitions(numPartitions)
    ordered.foreach { node =>
      val fragBuilder = OffloadFragment.newBuilder()
      // Inject file partitions into NativeScan leaves (reuse the existing helper).
      fragBuilder.setBlockProto(
        ByteString.copyFrom(CometExec.injectScanFilesFor(root, node.block)))
      node.inputs.foreach { ex =>
        val producer = blockOf(ex.child)
        val producerIdx = indexOf(producer)
        val keyOrdinals = hashKeyOrdinals(ex, producer.output, root)
        val inputBuilder = OffloadInput.newBuilder().setProducer(producerIdx)
        keyOrdinals.foreach(o => inputBuilder.addHashKeyOrdinals(o))
        fragBuilder.addInputs(inputBuilder)
      }
      planBuilder.addFragments(fragBuilder)
    }
    planBuilder.build().toByteArray
  }

  /** Map an exchange's HashPartitioning key expressions to ordinals in the producer's output. */
  private def hashKeyOrdinals(
      ex: CometShuffleExchangeExec,
      producerOutput: Seq[Attribute],
      root: SparkPlan): Seq[Int] = ex.outputPartitioning match {
    case HashPartitioning(expressions, _) =>
      expressions.map { e =>
        val attr = e match {
          case a: AttributeReference => a
          case other =>
            throw new UnsupportedOperationException(
              s"Comet Ballista offload: hash key is not a simple column ($other); not " +
                s"supported:\n$root")
        }
        val ord = producerOutput.indexWhere(_.exprId == attr.exprId)
        if (ord < 0) {
          throw new UnsupportedOperationException(
            s"Comet Ballista offload: hash key $attr not found in producer output " +
              s"${producerOutput.map(_.name)}:\n$root")
        }
        ord
      }
    case other =>
      throw new UnsupportedOperationException(
        s"Comet Ballista offload: only HashPartitioning exchanges are supported; found $other " +
          s"(range/single-partition sort is a future increment):\n$root")
  }
}
