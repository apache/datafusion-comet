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

import java.lang.reflect.Modifier

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.runtimeMirror

import org.scalatest.funsuite.AnyFunSuite

import org.apache.xbean.asm9.{ClassReader, Opcodes}
import org.apache.xbean.asm9.tree.{ClassNode, FieldInsnNode, MethodInsnNode}

import com.google.common.reflect.ClassPath

class CometPlanEqualitySuite extends AnyFunSuite {

  // Native serialization state and the source plan are deliberately not the identity of a
  // converted operator. Some operators do compare these; exclusion does not forbid comparison.
  private val commonExclusions = Set("nativeOp", "originalPlan")

  private val exclusions: Map[Class[_], Set[String]] = Map(
    // These orderings are derived from the sort/join inputs already compared by equals.
    classOf[CometSortExec] -> Set("outputOrdering"),
    classOf[CometHashJoinExec] -> Set("outputOrdering"),
    classOf[CometBroadcastHashJoinExec] -> Set("outputOrdering"),
    classOf[CometBroadcastNestedLoopJoinExec] -> Set("outputOrdering"),
    classOf[CometSortMergeJoinExec] -> Set("outputOrdering"),
    // Scan identity is delegated to originalPlan, with filters compared separately. scan and
    // sourceKey are execution plumbing cleared by doCanonicalize. Keep this list explicit so
    // a newly added parameter still requires a decision rather than exempting the entire scan.
    classOf[CometNativeScanExec] -> Set(
      "relation",
      "output",
      "requiredSchema",
      "optionalBucketSet",
      "optionalNumCoalescedBuckets",
      "tableIdentifier",
      "disableBucketedScan",
      "scan",
      "sourceKey"),
    // Equality compares originalPlan/child. The broadcast payload is Arrow batches, not a
    // Spark relation built using mode; canonicalization retains only the child's identity.
    classOf[CometBroadcastExchangeExec] -> Set("output", "mode"),
    // Deferred partition serialization state; metadataLocation/scanHashCode identify the scan.
    classOf[CometIcebergNativeScanExec] -> Set("nativeIcebergScanMetadata"))

  private def constructorParameters(cls: Class[_]): Set[String] =
    runtimeMirror(cls.getClassLoader)
      .classSymbol(cls)
      .primaryConstructor
      .asMethod
      .paramLists
      .flatten
      .map(_.name.decodedName.toString)
      .toSet

  /**
   * A structural guard: reads of constructor accessors/fields must occur in equals. This does not
   * prove those reads implement a correct comparison, nor detect semantics never captured as a
   * constructor parameter. Keep result-based exchange-reuse regressions as well.
   */
  private def parametersReadByEquals(cls: Class[_]): Set[String] = {
    val owner = cls.getMethod("equals", classOf[Object]).getDeclaringClass
    val hierarchy = Iterator
      .iterate[Class[_]](cls)(_.getSuperclass)
      .takeWhile(_ != null)
      .map(_.getName.replace('.', '/'))
      .toSet
    val stream = owner.getResourceAsStream(s"/${owner.getName.replace('.', '/')}.class")
    require(stream != null, s"Cannot read bytecode for ${owner.getName}")
    val node = new ClassNode()
    try {
      new ClassReader(stream).accept(node, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES)
    } finally {
      stream.close()
    }
    val equality = node.methods.asScala
      .find { method =>
        method.name == "equals" && method.desc == "(Ljava/lang/Object;)Z"
      }
      .getOrElse(fail(s"Cannot find equals bytecode for ${owner.getName}"))
    equality.instructions
      .iterator()
      .asScala
      .collect {
        case call: MethodInsnNode
            if hierarchy.contains(call.owner) && call.desc.startsWith("()") =>
          call.name
        case field: FieldInsnNode
            if field.getOpcode == Opcodes.GETFIELD && hierarchy.contains(field.owner) =>
          field.name
      }
      .toSet
  }

  test("native plan constructor parameters participate in equals or have explicit exclusions") {
    val loader = classOf[CometNativeExec].getClassLoader
    val productionLocation =
      classOf[CometNativeExec].getProtectionDomain.getCodeSource.getLocation
    val classes = ClassPath
      .from(loader)
      .getAllClasses
      .asScala
      .filter(_.getName.startsWith("org.apache.spark.sql.comet."))
      .map(_.load())
      .filter { cls =>
        !Modifier.isAbstract(cls.getModifiers) &&
        cls.getProtectionDomain.getCodeSource.getLocation == productionLocation &&
        (classOf[CometNativeExec].isAssignableFrom(cls) ||
          cls == classOf[CometBroadcastExchangeExec] || cls == classOf[CometUnionExec])
      }
      .toSeq
      .sortBy(_.getName)
    assert(classes.nonEmpty, "No Comet native plans discovered")
    assert(
      exclusions.keySet.subsetOf(classes.toSet),
      "Exclusions refer to undiscovered operators")

    val missing = classes.flatMap { cls =>
      val parameters = constructorParameters(cls)
      val excluded = exclusions.getOrElse(cls, Set.empty)
      assert(excluded.subsetOf(parameters), s"Stale constructor exclusions for ${cls.getName}")
      val reads = parametersReadByEquals(cls)
      if (cls == classOf[CometNativeScanExec] || cls == classOf[CometBroadcastExchangeExec]) {
        assert(
          reads.contains("originalPlan"),
          s"${cls.getName} must compare its delegated identity")
      }
      (parameters -- reads -- commonExclusions -- excluded).toSeq.sorted
        .map(parameter => s"${cls.getSimpleName}.$parameter")
    }
    assert(
      missing.isEmpty,
      "Constructor parameters missing from equals: " + missing.mkString(", ") +
        ". Compare them in equals or document why they do not affect plan identity in exclusions.")
  }

  test("the guard detects an omitted constructor parameter") {
    val cls = classOf[CometPlanEqualitySuite.IncompleteEquality]
    assert(constructorParameters(cls) -- parametersReadByEquals(cls) == Set("omitted"))
  }
}

object CometPlanEqualitySuite {
  private class IncompleteEquality(val compared: Int, val omitted: Boolean) {
    override def equals(other: Any): Boolean = other match {
      case that: IncompleteEquality => compared == that.compared
      case _ => false
    }
    override def hashCode(): Int = compared
  }
}
