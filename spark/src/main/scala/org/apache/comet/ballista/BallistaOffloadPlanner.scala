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
import org.apache.spark.sql.comet.{CometExec, CometHashJoinExec, CometNativeExec, CometSortMergeJoinExec}
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
 *   - a co-partitioned join block (shuffle-hash or sort-merge) fed by exactly two Comet hash
 *     exchanges, discovered via a generic DFS over the block that stops descent at each exchange.
 *     DFS pre-order naturally visits a binary join's left input before its right input, matching
 *     the join proto's `[left_leaf, right_leaf]` leaf order; no join-specific handling is
 *     required.
 *
 * Other multi-input DAG shapes (e.g. a native block fed by more than two upstream fragments, or
 * broadcast joins) are a future increment; the walker rejects anything it doesn't recognize with
 * an [[UnsupportedOperationException]] rather than guessing.
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

    // The direct native-block inputs of `p`'s subtree are the Comet hash exchanges reachable
    // without crossing a deeper native block, discovered by a plain DFS that stops descent at
    // each exchange. Used both at block level (to find a block's own inputs) and rooted at a
    // single join's left/right child (to validate how a join's two sides split those inputs).
    def directExchanges(p: SparkPlan): Seq[CometShuffleExchangeExec] = {
      val found = mutable.ArrayBuffer.empty[CometShuffleExchangeExec]
      def walk(p: SparkPlan): Unit = p match {
        case e: CometShuffleExchangeExec => found += e // do NOT descend past an exchange
        case other => other.children.foreach(walk)
      }
      walk(p)
      found.toSeq
    }

    // The binary Comet join nodes directly inside `p`'s subtree, discovered the same way (DFS,
    // stop descent at exchanges) but continuing past a join into its own children so a fused
    // MULTI-join block (two joins with no exchange between them) is still detected.
    def directJoins(p: SparkPlan): Seq[SparkPlan] = {
      val found = mutable.ArrayBuffer.empty[SparkPlan]
      def walk(p: SparkPlan): Unit = p match {
        case _: CometShuffleExchangeExec => // do NOT descend past an exchange
        case j @ (_: CometHashJoinExec | _: CometSortMergeJoinExec) =>
          found += j
          j.children.foreach(walk)
        case other => other.children.foreach(walk)
      }
      walk(p)
      found.toSeq
    }

    // Resolve a block's ordered DAG inputs (producer exchanges, in the proto's
    // `[left_leaf, right_leaf]` order) from its direct exchanges and joins. A block may be a
    // leaf (0 exchanges), a linear chain (1 exchange, no join -- e.g. partial->final
    // aggregate), or a single co-partitioned join whose two sides each contribute exactly one
    // of exactly two exchanges. Any other shape -- a fused multi-join block, a join with a
    // broadcast (non-hash-exchange) side, or exchanges not cleanly split by a single join --
    // throws rather than silently mis-pairing exchanges from different joins.
    def resolveInputs(block: CometNativeExec): Seq[CometShuffleExchangeExec] = {
      val exchanges = directExchanges(block)
      val joins = directJoins(block)
      (exchanges.size, joins.size) match {
        case (0, _) => Seq.empty
        case (1, 0) => exchanges
        case (2, 1) =>
          val join = joins.head
          val (leftPlan, rightPlan) = join match {
            case j: CometHashJoinExec => (j.left, j.right)
            case j: CometSortMergeJoinExec => (j.left, j.right)
          }
          val leftEx = directExchanges(leftPlan)
          val rightEx = directExchanges(rightPlan)
          if (leftEx.size == 1 && rightEx.size == 1 &&
            (leftEx.toSet ++ rightEx.toSet) == exchanges.toSet) {
            Seq(leftEx.head, rightEx.head)
          } else {
            throw new UnsupportedOperationException(
              "Comet Ballista offload: join block's two Comet exchanges are not cleanly split " +
                s"one-each across the join's left (${leftEx.size} exchange(s)) and right " +
                s"(${rightEx.size} exchange(s)) sides -- a broadcast join side is a future " +
                s"increment:\n$root")
          }
        case (n, m) =>
          throw new UnsupportedOperationException(
            s"Comet Ballista offload: block resolves to $n Comet exchange(s) and $m binary " +
              "join(s); only a leaf block (0 exchanges), a linear chain (1 exchange, no " +
              "join), or a single co-partitioned join (exactly 2 exchanges split one-each " +
              "across exactly one join's inputs) are supported -- a fused multi-join block " +
              s"is a future increment:\n$root")
      }
    }

    def register(block: CometNativeExec): Int = indexOf.getOrElseUpdate(
      block, {
        val inputs = resolveInputs(block)
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
      // Co-partition check: a two-input (join) block's inputs must be hash-partitioned to the
      // same width. Spark's EnsureRequirements guarantees this; assert to fail fast otherwise.
      if (node.inputs.size == 2) {
        val ns = node.inputs.map(_.outputPartitioning).collect { case HashPartitioning(_, n) =>
          n
        }
        require(
          ns.distinct.size == 1,
          s"Comet Ballista offload: join inputs are not co-partitioned to the same width " +
            s"($ns):\n$root")
      }
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
