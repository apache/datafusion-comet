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
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

import org.apache.comet.CometConf.COMET_ONHEAP_MEMORY_OVERHEAD
import org.apache.comet.ExpressionReference._
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Incompatible, QueryPlanSerde, Unsupported}

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  private val publicConfigs: Set[ConfigEntry[_]] = CometConf.allConfs.filter(_.isPublic).toSet

  /**
   * (expression class simple name, compatible notes, incompatible reasons, unsupported reasons)
   */
  private type CategoryNotes = Seq[(String, Seq[String], Seq[String], Seq[String])]

  /**
   * Mapping from expression category to the compatibility guide page where that category's
   * auto-generated notes should be written, along with a function that produces the notes for
   * that category from the serde maps in `QueryPlanSerde`.
   */
  private def categoryPages: Map[String, (String, () => CategoryNotes)] = Map(
    "array" -> ((
      "compatibility/expressions/array.md",
      () =>
        QueryPlanSerde.arrayExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "datetime" -> ((
      "compatibility/expressions/datetime.md",
      () =>
        QueryPlanSerde.temporalExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "math" -> ((
      "compatibility/expressions/math.md",
      () =>
        QueryPlanSerde.mathExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "struct" -> ((
      "compatibility/expressions/struct.md",
      () =>
        QueryPlanSerde.structExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "aggregate" -> ((
      "compatibility/expressions/aggregate.md",
      () =>
        QueryPlanSerde.aggrSerdeMap.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "string" -> ((
      "compatibility/expressions/string.md",
      () =>
        QueryPlanSerde.stringExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "map" -> ((
      "compatibility/expressions/map.md",
      () =>
        QueryPlanSerde.mapExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "misc" -> ((
      "compatibility/expressions/misc.md",
      () =>
        QueryPlanSerde.miscExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })),
    "url" -> ((
      "compatibility/expressions/url.md",
      () =>
        QueryPlanSerde.urlExpressions.toSeq.map { case (cls, serde) =>
          (
            cls.getSimpleName,
            serde.getCompatibleNotes(),
            serde.getIncompatibleReasons(),
            serde.getUnsupportedReasons())
        })))

  /**
   * Curated status for Spark built-ins that Comet does not serde-support. This list lives in
   * GenerateDocs (not in the serde files) on purpose: it is excluded from the heavy CI path
   * filters (build, spark-sql, iceberg) in dev/ci/compute-changes.py, so editing it (e.g. when an
   * issue is filed) does not trigger those heavy jobs. Keyed by Spark function name.
   */
  private val plannedExpressions: Map[String, PlannedExpr] = Map(
    "approx_count_distinct" -> PlannedExpr(Planned, issue = Some(4098)),
    "kurtosis" -> PlannedExpr(Planned, issue = Some(4098))
    // Populated to match the current doc during a later task.
  )

  /**
   * Spark function groups rendered as tables, in display order. Families that fall back wholesale
   * (xml_funcs, csv_funcs, geospatial, etc.) are intentionally omitted; they are covered by the
   * "Not currently planned" prose section.
   *
   * Consumed by `generateExpressionReference` (added in the following task).
   */
  private val expressionGroups: Seq[String] = Seq(
    "agg_funcs",
    "array_funcs",
    "bitwise_funcs",
    "collection_funcs",
    "conditional_funcs",
    "conversion_funcs",
    "datetime_funcs",
    "generator_funcs",
    "hash_funcs",
    "json_funcs",
    "lambda_funcs",
    "map_funcs",
    "math_funcs",
    "misc_funcs",
    "predicate_funcs",
    "string_funcs",
    "struct_funcs",
    "url_funcs",
    "window_funcs")

  /**
   * Map expression class -> compat-guide category, only for categories that have a page. Must
   * stay in sync with `categoryPages`: only categories that have a compat-guide page belong here,
   * so functions in other categories intentionally get no compat link.
   */
  private val classToCategory: Map[Class[_], String] = Seq(
    QueryPlanSerde.arrayExpressions.keys.map((_: Class[_]) -> "array"),
    QueryPlanSerde.temporalExpressions.keys.map((_: Class[_]) -> "datetime"),
    QueryPlanSerde.mathExpressions.keys.map((_: Class[_]) -> "math"),
    QueryPlanSerde.structExpressions.keys.map((_: Class[_]) -> "struct"),
    QueryPlanSerde.stringExpressions.keys.map((_: Class[_]) -> "string"),
    QueryPlanSerde.mapExpressions.keys.map((_: Class[_]) -> "map"),
    QueryPlanSerde.miscExpressions.keys.map((_: Class[_]) -> "misc"),
    QueryPlanSerde.urlExpressions.keys.map((_: Class[_]) -> "url"),
    QueryPlanSerde.aggrSerdeMap.keys.map((_: Class[_]) -> "aggregate")).flatten.toMap

  /** Build the serde-derived doc facts for a function class, if Comet serde-supports it. */
  private def serdeDocInfoFor(className: String): Option[SerdeDocInfo] = {
    // scalastyle:off classforname
    val clsOpt = Try(Class.forName(className)).toOption
    // scalastyle:on classforname
    clsOpt.flatMap { cls =>
      // The cast is erased at runtime; the lookup is by key equality, so a class that is
      // not actually an Expression subtype simply matches no key and yields None.
      val exprSerde = QueryPlanSerde.exprSerdeMap
        .get(cls.asInstanceOf[Class[_ <: Expression]])
      val aggSerde = QueryPlanSerde.aggrSerdeMap.get(cls)
      val notesAndSummary: Option[(Option[String], Boolean)] = exprSerde match {
        case Some(s) =>
          Some(
            (
              s.getExpressionSummary,
              s.getCompatibleNotes().nonEmpty || s.getIncompatibleReasons().nonEmpty ||
                s.getUnsupportedReasons().nonEmpty))
        case None =>
          aggSerde.map { s =>
            (
              s.getExpressionSummary,
              s.getCompatibleNotes().nonEmpty || s.getIncompatibleReasons().nonEmpty ||
                s.getUnsupportedReasons().nonEmpty)
          }
      }
      notesAndSummary.map { case (summary, hasCompat) =>
        // scalastyle:off caselocale
        val anchor = cls.getSimpleName.toLowerCase
        // scalastyle:on caselocale
        SerdeDocInfo(
          summary = summary,
          hasCompatContent = hasCompat,
          category = classToCategory.get(cls),
          anchor = anchor)
      }
    }
  }

  /**
   * Resolve all rows for a group, logging warnings for unclassified builtins.
   *
   * Consumed by `generateExpressionReference` (added in the following task).
   */
  private def rowsForGroup(group: String, entries: Seq[FunctionEntry]): Seq[ReferenceRow] = {
    entries.filter(_.group == group).map { e =>
      val (row, warn) =
        resolveRow(e, serdeDocInfoFor(e.className), plannedExpressions.get(e.name))
      // scalastyle:off println
      warn.foreach(w => println(s"[GenerateDocs][WARN] $w"))
      // scalastyle:on println
      row
    }
  }

  def main(args: Array[String]): Unit = {
    val userGuideLocation = args(0)
    generateConfigReference(s"$userGuideLocation/configs.md")
    generateCompatibilityGuide(s"$userGuideLocation/compatibility/expressions/cast.md")
    for ((category, (page, notesFn)) <- categoryPages) {
      generateExpressionCompatNotes(s"$userGuideLocation/$page", category, notesFn())
    }
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
    val sorted = notes.sortBy(_._1).filter { case (_, compat, incompat, unsupported) =>
      compat.nonEmpty || incompat.nonEmpty || unsupported.nonEmpty
    }
    for ((name, compat, incompat, unsupported) <- sorted) {
      w.write(s"\n## $name\n".getBytes)
      if (compat.nonEmpty) {
        w.write(
          ("\nThe following differences from Spark are always present and do not require" +
            " any additional configuration:\n\n").getBytes)
        for (note <- compat) {
          w.write(s"- $note\n".getBytes)
        }
      }
      if (incompat.nonEmpty) {
        w.write(
          (s"\nThe following incompatibilities cause `$name` to fall back to Spark by default." +
            s" Set `spark.comet.expression.$name.allowIncompatible=true` to enable Comet" +
            " acceleration despite these differences.\n\n").getBytes)
        for (reason <- incompat) {
          w.write(s"- $reason\n".getBytes)
        }
      }
      if (unsupported.nonEmpty) {
        w.write("\nThe following cases are not supported by Comet:\n\n".getBytes)
        for (reason <- unsupported) {
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
            case Compatible(notes) =>
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
