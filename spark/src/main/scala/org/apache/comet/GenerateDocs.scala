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

import java.io.{BufferedOutputStream, BufferedReader, FileOutputStream, FileReader}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Cast

import org.apache.comet.CometConf.COMET_ONHEAP_MEMORY_OVERHEAD
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{CodegenDispatchFallback, CometAggregateExpressionSerde, CometCodegenDispatch, CometExpressionSerde, Compatible, Incompatible, NativeOptInAvailable, QueryPlanSerde, Unsupported}

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  private val publicConfigs: Set[ConfigEntry[_]] = CometConf.allConfs.filter(_.isPublic).toSet

  /**
   * Documentation notes for a single expression.
   *
   * @param name
   *   expression class simple name
   * @param compatibleNotes
   *   differences from Spark that are always present
   * @param incompatibleReasons
   *   reasons the native implementation is incompatible with Spark
   * @param unsupportedReasons
   *   cases that Comet's native implementation does not handle
   * @param nativeOptIn
   *   whether the serde implements `NativeOptInAvailable`, meaning the expression runs a
   *   Spark-compatible path by default and the user can opt into a native path
   * @param nativeOptInConfigKey
   *   the config key the user sets to opt into the native path
   * @param codegenDispatchFallback
   *   whether the serde mixes in `CodegenDispatchFallback`, meaning `unsupportedReasons` cases
   *   route through the JVM codegen dispatcher instead of falling back to Spark
   */
  private case class ExprNotes(
      name: String,
      compatibleNotes: Seq[String],
      incompatibleReasons: Seq[String],
      unsupportedReasons: Seq[String],
      nativeOptIn: Boolean,
      nativeOptInConfigKey: String,
      codegenDispatchFallback: Boolean)

  private type CategoryNotes = Seq[ExprNotes]

  /** Build the documentation notes for a single expression serde. */
  private def exprNotes(cls: Class[_], serde: CometExpressionSerde[_]): ExprNotes = {
    val optIn = serde.isInstanceOf[NativeOptInAvailable]
    val key = serde match {
      case n: NativeOptInAvailable =>
        n.nativeOptInConfigKeyOverride.getOrElse(CometConf.getExprAllowIncompatConfigKey(cls))
      case _ => CometConf.getExprAllowIncompatConfigKey(cls)
    }
    ExprNotes(
      cls.getSimpleName,
      serde.getCompatibleNotes(),
      serde.getIncompatibleReasons(),
      serde.getUnsupportedReasons(),
      optIn,
      key,
      codegenDispatchFallback = serde.isInstanceOf[CodegenDispatchFallback])
  }

  /** Build the documentation notes for a single aggregate expression serde. */
  private def aggExprNotes(cls: Class[_], serde: CometAggregateExpressionSerde[_]): ExprNotes =
    ExprNotes(
      cls.getSimpleName,
      serde.getCompatibleNotes(),
      serde.getIncompatibleReasons(),
      serde.getUnsupportedReasons(),
      // Aggregate serdes do not have a native opt-in path.
      nativeOptIn = false,
      nativeOptInConfigKey = CometConf.getExprAllowIncompatConfigKey(cls),
      codegenDispatchFallback = false)

  /**
   * Mapping from expression category to the compatibility guide filename where that category's
   * auto-generated notes should be written, along with a function that produces the notes for
   * that category from the serde maps in `QueryPlanSerde`. Filenames are resolved relative to the
   * per-Spark-version compatibility/expressions directory.
   */
  private def categoryPages: Map[String, (String, () => CategoryNotes)] = Map(
    "array" -> ("array.md",
    () =>
      QueryPlanSerde.arrayExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "datetime" -> ("datetime.md",
    () =>
      QueryPlanSerde.temporalExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "math" -> ("math.md",
    () =>
      QueryPlanSerde.mathExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "struct" -> ("struct.md",
    () =>
      QueryPlanSerde.structExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "aggregate" -> ("aggregate.md",
    () =>
      QueryPlanSerde.aggrSerdeMap.toSeq.map { case (cls, serde) =>
        aggExprNotes(cls, serde)
      }),
    "string" -> ("string.md",
    () =>
      QueryPlanSerde.stringExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "map" -> ("map.md",
    () =>
      QueryPlanSerde.mapExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "misc" -> ("misc.md",
    () =>
      QueryPlanSerde.miscExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }),
    "url" -> ("url.md",
    () =>
      QueryPlanSerde.urlExpressions.toSeq.map { case (cls, serde) =>
        exprNotes(cls, serde)
      }))

  /**
   * Args:
   *   - args(0): user guide root directory (e.g. `docs/source/user-guide/latest/`).
   *   - args(1) (optional): per-Spark-version subdirectory for compatibility pages (e.g.
   *     `spark-3.4`). When omitted, compat pages are written to `compatibility/expressions/`
   *     directly, preserving the legacy flat layout used by released-version doc trees.
   */
  def main(args: Array[String]): Unit = {
    val userGuideLocation = args(0)
    val compatPagesDir = if (args.length > 1) {
      s"$userGuideLocation/compatibility/expressions/${args(1)}"
    } else {
      s"$userGuideLocation/compatibility/expressions"
    }
    generateConfigReference(s"$userGuideLocation/configs.md")
    generateCompatibilityGuide(s"$compatPagesDir/cast.md")
    updateExpressionsPageImplementation(s"$userGuideLocation/expressions.md")
    for ((category, (filename, notesFn)) <- categoryPages) {
      generateExpressionCompatNotes(s"$compatPagesDir/$filename", category, notesFn())
    }
  }

  private val ImplNative = "Native"
  private val ImplCodegen = "Codegen dispatch"
  private val ImplHybrid = "Hybrid"
  private val ImplUnknown: String = new String(Array(8212.toChar))

  /**
   * Classify an expression serde by which execution path it exposes.
   *
   *   - `NativeOptInAvailable` (and its subtype `CodegenDispatchFallback`) means the serde has
   *     both a native path and a Spark-compatible (typically codegen-dispatch) path.
   *   - A plain `CometCodegenDispatch` means the serde has only a codegen-dispatch path.
   *   - Everything else is a plain native serde.
   */
  private def classifySerde(serde: CometExpressionSerde[_]): String = {
    if (serde.isInstanceOf[NativeOptInAvailable]) ImplHybrid
    else if (serde.isInstanceOf[CometCodegenDispatch[_]]) ImplCodegen
    else ImplNative
  }

  /**
   * Build a map from Spark built-in SQL function name (e.g. `array_max`) to its implementation
   * kind, resolving each name through Spark's `FunctionRegistry` to the backing Catalyst
   * expression class and then looking that class up in `QueryPlanSerde`'s serde maps.
   */
  private def buildFunctionNameToKind(): Map[String, String] = {
    val classNameToKind: Map[String, String] = {
      val exprKinds = QueryPlanSerde.exprSerdeMap.iterator.map { case (cls, serde) =>
        cls.getName -> classifySerde(serde)
      }
      // Aggregate serdes have no codegen-dispatch or opt-in path; they are always native.
      val aggKinds = QueryPlanSerde.aggrSerdeMap.iterator.map { case (cls, _) =>
        cls.getName -> ImplNative
      }
      (exprKinds ++ aggKinds).toMap
    }
    FunctionRegistry.expressions.iterator.map { case (name, (info, _)) =>
      name -> classNameToKind.getOrElse(info.getClassName, ImplUnknown)
    }.toMap
  }

  /**
   * Update the Implementation column of every markdown table in `expressions.md` in place.
   *
   * The generator recognises table blocks by their header row (`| Function | Status |
   * Implementation | Notes |`), skips the separator row, and rewrites the Implementation cell of
   * each data row based on the function name in the first column. Rows whose function name is
   * either not registered in Spark's `FunctionRegistry` or has no serde in `QueryPlanSerde`
   * (planned, rewritten to another expression, or operator-level) get the em-dash placeholder.
   */
  private def updateExpressionsPageImplementation(filename: String): Unit = {
    val nameToKind = buildFunctionNameToKind()
    val lines = readFileVerbatim(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    var implColumnIdx = -1
    var afterSeparator = false
    for (line <- lines) {
      val trimmed = line.trim
      if (trimmed.startsWith("|")) {
        val cells = splitTableRow(trimmed)
        val headerIdx = cells.indexWhere(_.trim == "Implementation")
        val isHeader = headerIdx >= 0 && cells.exists(_.trim == "Function") &&
          cells.exists(_.trim == "Status") && cells.exists(_.trim == "Notes")
        val isSeparator =
          implColumnIdx >= 0 && cells.length > 1 &&
            cells.drop(1).dropRight(1).forall(c => c.trim.matches("-+"))
        if (isHeader) {
          implColumnIdx = headerIdx
          afterSeparator = false
          w.write(s"${line.stripTrailing()}\n".getBytes)
        } else if (isSeparator) {
          afterSeparator = true
          w.write(s"${line.stripTrailing()}\n".getBytes)
        } else if (afterSeparator && implColumnIdx >= 0 && cells.length > implColumnIdx) {
          // Data row: extract SQL function name from column 1 (0-indexed cells: [empty, name,
          // status, impl, notes, empty]). Unescape any `\|` back to `|` since markdown requires
          // pipes inside table cells to be backslash-escaped.
          val nameCell = cells(1).trim
          val name = nameCell.stripPrefix("`").stripSuffix("`").replace("\\|", "|")
          val kind = nameToKind.getOrElse(name, ImplUnknown)
          val updated = cells.updated(implColumnIdx, s" $kind ")
          w.write(s"${updated.mkString("|").stripTrailing()}\n".getBytes)
        } else {
          w.write(s"${line.stripTrailing()}\n".getBytes)
        }
      } else {
        // Any non-table line ends the current table block.
        implColumnIdx = -1
        afterSeparator = false
        w.write(s"${line.stripTrailing()}\n".getBytes)
      }
    }
    w.close()
  }

  /**
   * Split a markdown table row on `|`. Ordinary `String.split` drops trailing empty tokens, so `|
   * a | b |` would come back as three elements instead of four. Passing `-1` as the limit
   * preserves them, which we need so that column indices stay aligned with the header row.
   *
   * `\|` (the markdown escape for a literal pipe inside a cell) is temporarily replaced with a
   * placeholder before splitting so the escaped pipe is not treated as a column boundary.
   */
  private def splitTableRow(row: String): Array[String] = {
    val placeholder = "\u0001"
    row.replace("\\|", placeholder).split("\\|", -1).map(_.replace(placeholder, "\\|"))
  }

  /** Read a file without stripping the BEGIN/END autogen markers used elsewhere. */
  private def readFileVerbatim(filename: String): Seq[String] = {
    val r = new BufferedReader(new FileReader(filename))
    val buffer = new ListBuffer[String]()
    var line = r.readLine()
    while (line != null) {
      buffer += line
      line = r.readLine()
    }
    r.close()
    buffer.toSeq
  }

  private def generateConfigReference(filename: String): Unit = {
    val pattern = "<!--BEGIN:CONFIG_TABLE\\[(.*)]-->".r
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      line match {
        case pattern(category) =>
          w.write("<!-- prettier-ignore-start -->\n".getBytes)
          w.write("| Config | Description | Default Value |\n".getBytes)
          w.write("|--------|-------------|---------------|\n".getBytes)
          category match {
            case "enable_expr" =>
              for (expr <- QueryPlanSerde.exprSerdeMap.keys.map(_.getSimpleName).toList.sorted) {
                val config = s"spark.comet.expression.$expr.enabled"
                w.write(
                  s"| `$config` | Enable Comet acceleration for `$expr` | true |\n".getBytes)
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
            case "enable_agg_expr" =>
              for (expr <- QueryPlanSerde.aggrSerdeMap.keys.map(_.getSimpleName).toList.sorted) {
                val config = s"spark.comet.expression.$expr.enabled"
                w.write(
                  s"| `$config` | Enable Comet acceleration for `$expr` | true |\n".getBytes)
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
            case _ =>
              val urlPattern = """Comet\s+(Compatibility|Tuning|Tracing)\s+Guide\s+\(""".r
              val confs = publicConfigs.filter(_.category == category).toList.sortBy(_.key)
              for (conf <- confs) {
                // convert links to Markdown
                val doc =
                  urlPattern.replaceAllIn(conf.doc.trim, m => s"[Comet ${m.group(1)} Guide](")
                // append env var info if present
                val docWithEnvVar = conf.envVar match {
                  case Some(envVarName) =>
                    s"$doc It can be overridden by the environment variable `$envVarName`."
                  case None => doc
                }
                if (conf.defaultValue.isEmpty) {
                  w.write(s"| `${conf.key}` | $docWithEnvVar | |\n".getBytes)
                } else {
                  val isBytesConf = conf.key == COMET_ONHEAP_MEMORY_OVERHEAD.key
                  if (isBytesConf) {
                    val bytes = conf.defaultValue.get.asInstanceOf[Long]
                    w.write(s"| `${conf.key}` | $docWithEnvVar | $bytes MiB |\n".getBytes)
                  } else {
                    val defaultVal = conf.defaultValueString
                    w.write(s"| `${conf.key}` | $docWithEnvVar | $defaultVal |\n".getBytes)
                  }
                }
              }
              w.write("<!-- prettier-ignore-end -->\n".getBytes)
          }
        case _ =>
      }
    }
    w.close()
  }

  private def generateCompatibilityGuide(filename: String): Unit = {
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == "<!--BEGIN:CAST_LEGACY_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.LEGACY)
      } else if (line.trim == "<!--BEGIN:CAST_TRY_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.TRY)
      } else if (line.trim == "<!--BEGIN:CAST_ANSI_TABLE-->") {
        writeCastMatrixForMode(w, CometEvalMode.ANSI)
      }
    }
    w.close()
  }

  private def generateExpressionCompatNotes(
      filename: String,
      category: String,
      notes: CategoryNotes): Unit = {
    val beginTag = s"<!--BEGIN:EXPR_COMPAT[$category]-->"
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == beginTag) {
        writeExpressionCompatNotes(w, notes)
      }
    }
    w.close()
  }

  private def writeExpressionCompatNotes(w: BufferedOutputStream, notes: CategoryNotes): Unit = {
    val sorted = notes.sortBy(_.name).filter { n =>
      n.compatibleNotes.nonEmpty || n.incompatibleReasons.nonEmpty || n.unsupportedReasons.nonEmpty
    }
    for (n <- sorted) {
      val name = n.name
      w.write(s"\n## $name\n".getBytes)
      if (n.compatibleNotes.nonEmpty) {
        w.write(
          ("\nThe following differences from Spark are always present and do not require" +
            " any additional configuration:\n\n").getBytes)
        for (note <- n.compatibleNotes) {
          w.write(s"- $note\n".getBytes)
        }
      }
      if (n.incompatibleReasons.nonEmpty) {
        val header = if (n.nativeOptIn) {
          s"\nBy default, `$name` is evaluated in the JVM using Spark's own code-generated" +
            " implementation (run inside the Comet pipeline), which matches Spark exactly." +
            s" Set `${n.nativeOptInConfigKey}=true` to opt into Comet's native implementation" +
            " instead, which has the following differences from Spark:\n\n"
        } else {
          s"\nThe following incompatibilities cause `$name` to fall back to Spark by default." +
            s" Set `spark.comet.expression.$name.allowIncompatible=true` to enable Comet" +
            " acceleration despite these differences.\n\n"
        }
        w.write(header.getBytes)
        for (reason <- n.incompatibleReasons) {
          w.write(s"- $reason\n".getBytes)
        }
      }
      if (n.unsupportedReasons.nonEmpty) {
        val header = if (n.codegenDispatchFallback) {
          "\nThe following cases have no native implementation and always run in the JVM using" +
            " Spark's code-generated implementation (inside the Comet pipeline):\n\n"
        } else {
          "\nThe following cases are not supported by Comet and always fall back to Spark," +
            " regardless of any `allowIncompatible` setting:\n\n"
        }
        w.write(header.getBytes)
        for (reason <- n.unsupportedReasons) {
          w.write(s"- $reason\n".getBytes)
        }
      }
    }
  }

  private def writeCastMatrixForMode(w: BufferedOutputStream, mode: CometEvalMode.Value): Unit = {
    val sortedTypes = CometCast.supportedTypes.sortBy(_.typeName)
    val typeNames = sortedTypes.map(_.typeName.replace("(10,2)", ""))

    // Collect annotations for meaningful notes
    val annotations = mutable.ListBuffer[(String, String, String)]()

    w.write("<!-- prettier-ignore-start -->\n".getBytes)

    // Write header row
    w.write("| |".getBytes)
    for (toTypeName <- typeNames) {
      w.write(s" $toTypeName |".getBytes)
    }
    w.write("\n".getBytes)

    // Write separator row
    w.write("|---|".getBytes)
    for (_ <- typeNames) {
      w.write("---|".getBytes)
    }
    w.write("\n".getBytes)

    // Write data rows
    for ((fromType, fromTypeName) <- sortedTypes.zip(typeNames)) {
      w.write(s"| $fromTypeName |".getBytes)
      for ((toType, toTypeName) <- sortedTypes.zip(typeNames)) {
        val cell = if (fromType == toType) {
          "-"
        } else if (!Cast.canCast(fromType, toType)) {
          "N/A"
        } else {
          val supportLevel = CometCast.isSupported(fromType, toType, None, mode)
          supportLevel match {
            case Compatible(notes, _) =>
              notes.filter(_.trim.nonEmpty).foreach { note =>
                annotations += ((fromTypeName, toTypeName, note.trim.replace("(10,2)", "")))
              }
              "C"
            case Incompatible(notes) =>
              notes.filter(_.trim.nonEmpty).foreach { note =>
                annotations += ((fromTypeName, toTypeName, note.trim.replace("(10,2)", "")))
              }
              "I"
            case Unsupported(_) =>
              "U"
          }
        }
        w.write(s" $cell |".getBytes)
      }
      w.write("\n".getBytes)
    }

    // Write annotations if any
    if (annotations.nonEmpty) {
      w.write("\n**Notes:**\n".getBytes)
      for ((from, to, note) <- annotations.distinct) {
        w.write(s"- **$from -> $to**: $note\n".getBytes)
      }
    }

    w.write("<!-- prettier-ignore-end -->\n".getBytes)
  }

  /** Read file into memory */
  private def readFile(filename: String): Seq[String] = {
    val r = new BufferedReader(new FileReader(filename))
    val buffer = new ListBuffer[String]()
    var line = r.readLine()
    var skipping = false
    while (line != null) {
      if (line.startsWith("<!--BEGIN:")) {
        buffer += line
        skipping = true
      } else if (line.startsWith("<!--END:")) {
        buffer += line
        skipping = false
      } else if (!skipping) {
        buffer += line
      }
      line = r.readLine()
    }
    r.close()
    buffer.toSeq
  }
}
