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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.Cast

import org.apache.comet.expressions.{CometCast, CometEvalMode, Compatible, Incompatible}

/**
 * Utility for generating markdown documentation from the configs.
 *
 * This is invoked when running `mvn clean package -DskipTests`.
 */
object GenerateDocs {

  def main(args: Array[String]): Unit = {
    generateConfigReference()
    generateCompatibilityGuide()
  }

  private def generateConfigReference(): Unit = {
    val filename = "docs/source/user-guide/configs.md"
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == "<!--BEGIN:CONFIG_TABLE-->") {
        val publicConfigs = CometConf.allConfs.filter(_.isPublic)
        val confs = publicConfigs.sortBy(_.key)
        w.write("| Config | Description | Default Value |\n".getBytes)
        w.write("|--------|-------------|---------------|\n".getBytes)
        for (conf <- confs) {
          if (conf.defaultValue.isEmpty) {
            w.write(s"| ${conf.key} | ${conf.doc.trim} | |\n".getBytes)
          } else {
            w.write(s"| ${conf.key} | ${conf.doc.trim} | ${conf.defaultValueString} |\n".getBytes)
          }
        }
      }
    }
    w.close()
  }

  private def generateCompatibilityGuide(): Unit = {
    val filename = "docs/source/user-guide/compatibility.md"
    val lines = readFile(filename)
    val w = new BufferedOutputStream(new FileOutputStream(filename))
    for (line <- lines) {
      w.write(s"${line.stripTrailing()}\n".getBytes)
      if (line.trim == "<!--BEGIN:COMPAT_CAST_TABLE-->") {
        w.write("| From Type | To Type | Notes |\n".getBytes)
        w.write("|-|-|-|\n".getBytes)
        for (fromType <- CometCast.supportedTypes) {
          for (toType <- CometCast.supportedTypes) {
            if (Cast.canCast(fromType, toType) && (fromType != toType || fromType.typeName
                .contains("decimal"))) {
              val fromTypeName = fromType.typeName.replace("(10,2)", "")
              val toTypeName = toType.typeName.replace("(10,2)", "")
              CometCast.isSupported(fromType, toType, None, CometEvalMode.LEGACY) match {
                case Compatible(notes) =>
                  val notesStr = notes.getOrElse("").trim
                  w.write(s"| $fromTypeName | $toTypeName | $notesStr |\n".getBytes)
                case _ =>
              }
            }
          }
        }
      } else if (line.trim == "<!--BEGIN:INCOMPAT_CAST_TABLE-->") {
        w.write("| From Type | To Type | Notes |\n".getBytes)
        w.write("|-|-|-|\n".getBytes)
        for (fromType <- CometCast.supportedTypes) {
          for (toType <- CometCast.supportedTypes) {
            if (Cast.canCast(fromType, toType) && fromType != toType) {
              val fromTypeName = fromType.typeName.replace("(10,2)", "")
              val toTypeName = toType.typeName.replace("(10,2)", "")
              CometCast.isSupported(fromType, toType, None, CometEvalMode.LEGACY) match {
                case Incompatible(notes) =>
                  val notesStr = notes.getOrElse("").trim
                  w.write(s"| $fromTypeName | $toTypeName  | $notesStr |\n".getBytes)
                case _ =>
              }
            }
          }
        }
      }
    }
    w.close()
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
