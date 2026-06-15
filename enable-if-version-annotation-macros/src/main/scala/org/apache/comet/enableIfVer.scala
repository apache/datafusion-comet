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

import scala.annotation.{compileTimeOnly, nowarn, StaticAnnotation}
import scala.language.experimental._
import scala.reflect.macros.whitebox

import org.semver4j.{RangesListFactory, Semver}

/**
 * Shared machinery behind the version annotations [[enableIfVer]], [[implementIfVer]] and
 * [[enableOverrideIfVer]].
 *
 * Every annotation performs the same compile-time tree rewrite (drop / empty-body / strip
 * `override`). the ONLY thing that differs is a single `Boolean` - does the build's targeted
 * version satisfy the given range? So the match decision is computed per annotation and handed to
 * the shared expansions here.
 *
 * The build feeds the targeted version of each dimension to the macro via
 * `-Xmacro-settings:enableIfVer.<dimension>=<version>`, read at expansion time from `c.settings`
 * (see [[versionOf]]). Adding a dimension is therefore just one more build entry - no generated
 * sources. Comet currently configures a single dimension, `spark`.
 *
 * Ranges are matched by <a href="https://github.com/semver4j/semver4j">semver4j</a>: full
 * `major.minor.patch` versions with `>` `>=` `<` `<=` `=` `!=`, space = AND, `||` = OR, `A - B`
 * hyphen ranges and more.
 */
object EnableIfVerSupport {

  /**
   * Does the configured `version` satisfy the semver `range`?
   *
   * Pre-release / build metadata is dropped before matching: a build targeting a pre-release
   * Spark (e.g. `4.2.0-preview4`) should still select the code gated for that major.minor.patch
   * line (`>=3.5.0`, `4`, ...). semver4j, like node-semver, otherwise refuses to match a
   * pre-release version against a range whose comparators carry no pre-release of the same tuple.
   */
  def satisfies(range: String, version: String): Boolean = {
    val v = new Semver(version)
    val core = new Semver(s"${v.getMajor}.${v.getMinor}.${v.getPatch}")
    core.satisfies(RangesListFactory.create(range.trim))
  }

  /** Prefix of the `-Xmacro-settings` keys this macro understands. */
  private val SettingPrefix = "enableIfVer."

  /** Parse `enableIfVer.<dimension>=<version>` entries out of `-Xmacro-settings`. */
  private def configuredVersions(c: whitebox.Context): Map[String, String] =
    c.settings.collect {
      case s if s.startsWith(SettingPrefix) =>
        s.stripPrefix(SettingPrefix).split("=", 2) match {
          case Array(dim, ver) => dim -> parseVersionFromSetting(dim, ver)
          case _ =>
            c.abort(
              c.enclosingPosition,
              s"@enableIfVer: malformed macro setting '$s' " +
                s"(expected $SettingPrefix<dimension>=<version>)")
        }
    }.toMap

  private def parseVersionFromSetting(name: String, version: String): String = {
    try {
      // Not using Semver.parse as it will return null instead of giving us meaningful
      // exceptions on invalid input
      new Semver(version)

      version
    } catch {
      case e: Throwable =>
        sys.error(
          s"malformed version passed in macro setting for '$name', expected a valid " +
            s"SemVer got '$version' (error: ${e.toString})")
    }
  }

  /**
   * Build-time version configured for `dimension`, read from `-Xmacro-settings`. The single
   * extensibility seam: to add a dimension, pass
   * `-Xmacro-settings:${SettingPrefix}<dimension>=<version>` from the build.
   *
   * Aborts compilation when the dimension was not configured at all.
   */
  private def versionOf(c: whitebox.Context, dimension: String): String = {
    val versions = configuredVersions(c)
    versions.getOrElse(
      dimension,
      // we do not treat a missing dimension as a match since we want to avoid silent failures
      c.abort(
        c.enclosingPosition,
        s"@enableIfVer: no version configured for dimension '$dimension'" +
          s" (configured: ${versions.keys.toList.sorted.mkString(", ")}). " +
          "Pass it via the compiler flag " +
          s""""-Xmacro-settings:$SettingPrefix$dimension=<version>"."""))
  }

  object Macros {

    /**
     * Extract the named `dimension = range` argument of a version annotation. A named argument is
     * `name = value`, whose immediate children are `[Ident(name), value]` - matched structurally
     * so this works on both Scala 2.12 (`AssignOrNamedArg`) and 2.13 (`NamedArg`). A positional
     * arg (e.g. a bare literal) has no such children and is rejected.
     */
    private def namedRanges(c: whitebox.Context): List[(String, String)] = {
      import c.universe._
      val args = c.macroApplication match {
        case Apply(Select(Apply(_, as), _), _) => as
      }

      args.map { arg =>
        arg.children match {
          case List(Ident(name), value) =>
            (name.decodedName.toString, c.eval(c.Expr[String](value)))
          case _ =>
            c.abort(
              c.enclosingPosition,
              "@enableIfVer (and all the related version annotations) require named " +
                s"""arguments, e.g. spark = ">=3.5.0"". got: ${showRaw(arg)}""")
        }
      }
    }

    /** Require exactly one named dimension arg and return whether the build matches its range. */
    def singleKeep(c: whitebox.Context, specificMacroPrefix: String): Boolean = {
      val ranges = namedRanges(c)
      if (ranges.size != 1) {
        val ifCase = if (specificMacroPrefix.isEmpty) "enableIf" else "If"
        c.abort(
          c.enclosingPosition,
          s"@${specificMacroPrefix}${ifCase}Ver accepts exactly one dimension " +
            s"(got ${ranges.size}).")
      }
      val (dim, range) = ranges.head
      satisfies(range, versionOf(c, dim))
    }

    // ----- generic tree-rewrite expansions (take a precomputed keep) ---------------------------

    /** Keep the annotated member as-is when `keep`, otherwise drop it entirely. */
    def enable(c: whitebox.Context)(annottees: Seq[c.Expr[Any]])(keep: Boolean): c.Expr[Any] = {
      import c.universe._
      if (keep) c.Expr[Any](q"..$annottees")
      else c.Expr(EmptyTree)
    }

    /**
     * Keep the annotated member as-is when `keep`, otherwise remove the class body and the
     * inheritance
     */
    def implementIf(c: whitebox.Context)(annottees: Seq[c.Expr[Any]])(
        keep: Boolean): c.Expr[Any] = {
      import c.universe._

      if (keep) return c.Expr[Any](q"..$annottees")
      val head = annottees.head.tree match {
        case ClassDef(mods, name, tparams, Template(_, self, _)) =>
          ClassDef(mods, name, tparams, Template(List(), self, List(EmptyTree)))
        case ModuleDef(mods, name, Template(_, self, _)) =>
          ModuleDef(mods, name, Template(List(), self, List(EmptyTree)))
      }
      c.Expr(q"$head; ..${annottees.tail}")
    }

    /** Keep the def/val as-is when `keep`. otherwise strip its `override` modifier. */
    def enableOverride(c: whitebox.Context)(annottees: Seq[c.Expr[Any]])(
        keep: Boolean): c.Expr[Any] = {
      import c.universe._
      import scala.reflect.internal.Flags
      if (keep) return c.Expr[Any](q"..$annottees")

      val head = annottees.head.tree match {
        case DefDef(mods, name, tparams, vparams, tpt, rhs) =>
          val newMods = Modifiers(
            (mods.flags.asInstanceOf[Long] & ~Flags.OVERRIDE).asInstanceOf[FlagSet],
            mods.privateWithin,
            mods.annotations)
          DefDef(newMods, name, tparams, vparams, tpt, rhs)

        case ValDef(mods, name, tpt, rhs) =>
          val newMods = Modifiers(
            (mods.flags.asInstanceOf[Long] & ~Flags.OVERRIDE).asInstanceOf[FlagSet],
            mods.privateWithin,
            mods.annotations)
          ValDef(newMods, name, tpt, rhs)
      }
      c.Expr(q"$head; ..${annottees.tail}")
    }
  }
}

object enableIfVer {
  object Macros {
    def verEnable(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] =
      EnableIfVerSupport.Macros.enable(c)(annottees)(EnableIfVerSupport.Macros.singleKeep(c, ""))
    def verImplementIf(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] =
      EnableIfVerSupport.Macros.implementIf(c)(annottees)(
        EnableIfVerSupport.Macros.singleKeep(c, "implement"))
    def verEnableOverride(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] =
      EnableIfVerSupport.Macros.enableOverride(c)(annottees)(
        EnableIfVerSupport.Macros.singleKeep(c, "enableOverride"))
  }
}

/**
 * Keep the annotated member only when the build matches a single dimension's range. otherwise
 * drop it entirely. Exactly one dimension must be given as a named semver range. Known dimensions
 * are whatever the build configures via `-Xmacro-settings` (currently `spark`).
 *
 * Example:
 * {{{
 * @enableIfVer(spark = ">=3.5.0")             // keep only on Spark 3.5+
 * def onlyOn35Plus(): Unit = ...
 *
 * @enableIfVer(spark = ">=3.4.0 <4.0.0")      // keep on the 3.4 / 3.5 line, drop on 4.0
 * override protected def withNewChildInternal(c: SparkPlan): SparkPlan = copy(child = c)
 * }}}
 */
@nowarn("cat=unused") // params are used by the macro
@compileTimeOnly("enable macro paradise to expand macro annotations")
final class enableIfVer(spark: String = "") extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro enableIfVer.Macros.verEnable
}

/**
 * Like [[enableIfVer]], but on a non-matching version the class/object is KEPT (so the type still
 * exists) with its body emptied. The inheritance is removed. Use this when the type must stay
 * referenceable on every version but its body touches symbols that only exist on some versions.
 *
 * For example, `WindowGroupLimitExec` was added in Spark 3.5, so this does not compile on 3.4 -
 * the top-level import and the parameter type are resolved on every version:
 * {{{
 * import org.apache.spark.sql.execution.window.WindowGroupLimitExec
 *
 * class RunWithWGL {
 *   def run(a: WindowGroupLimitExec): Unit = ...
 * }
 * }}}
 *
 * Gate the class so its body (and the 3.5-only import, scoped inside it) only exists on 3.5+:
 * {{{
 * @implementIfVer(spark = ">=3.5")
 * class RunWithWGL {
 *   import org.apache.spark.sql.execution.window.WindowGroupLimitExec
 *   def run(a: WindowGroupLimitExec): Unit = ...
 * }
 * }}}
 *
 * this will also remove inheritance so `isSomething` won't be required to implement when only
 * removing the class body
 * {{{
 * abstract class Base {
 *   protected val isSomething: Boolean
 *   def run(): Unit = if (isSomething) println("something")
 * }
 *
 * @implementIfVer(spark = ">=4")
 * class OnlySpark4OrAbove extends Base {
 *   protected val isSomething: Boolean = false
 *
 *   override def run(): Unit = println("only for spark 4+")  // dropped on < 4.0
 * }
 *
 * // For spark below 4 the expanded code will be
 * class OnlySpark4OrAbove {
 * }
 * }}}
 */
@nowarn("cat=unused") // params are used by the macro
@compileTimeOnly("enable macro paradise to expand macro annotations")
final class implementIfVer(spark: String = "") extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro enableIfVer.Macros.verImplementIf
}

/**
 * Like [[enableIfVer]], but on a non-matching version the `override` modifier is stripped from
 * the annotated def/val instead of removing it. Use this for a member that overrides a base
 * member only on some versions (because the base member only exists there).
 *
 * Example:
 * {{{
 * // `withNewChildInternal` only exists in the base on Spark 3.2+. On < 3.2 the `override` is
 * // stripped and it becomes a plain (non-overriding) def, so it still compiles.
 * @enableOverrideIfVer(spark = ">=3.2")
 * override def withNewChildInternal(c: SparkPlan): SparkPlan = copy(child = c)
 * }}}
 */
@nowarn("cat=unused") // params are used by the macro
@compileTimeOnly("enable macro paradise to expand macro annotations")
final class enableOverrideIfVer(spark: String = "") extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro enableIfVer.Macros.verEnableOverride
}
