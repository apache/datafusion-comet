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

import org.apache.spark.sql.catalyst.expressions.Cast

import org.apache.comet.CometConf.COMET_ONHEAP_MEMORY_OVERHEAD
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Incompatible, QueryPlanSerde, Unsupported}

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  private val publicConfigs: Set[ConfigEntry[_]] = CometConf.allConfs.filter(_.isPublic).toSet

  def main(args: Array[String]): Unit = {
    val userGuideLocation = args(0)
    generateConfigReference(s"$userGuideLocation/configs.md")
    generateCompatibilityGuide(s"$userGuideLocation/compatibility.md")
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

    w.write("<!-- prettier-ignore-end -->\n".getBytes)

    // Write annotations if any
    if (annotations.nonEmpty) {
      w.write("\n**Notes:**\n".getBytes)
      for ((from, to, note) <- annotations.distinct) {
        w.write(s"- **$from -> $to**: $note\n".getBytes)
      }
    }
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
