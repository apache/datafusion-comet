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

package org.apache.comet.serde

import java.time.ZoneOffset

import scala.util.Try

import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
 * Converts the timezone Spark stamps on an expression into an ID that native code can parse.
 *
 * Spark resolves timezone IDs with `ZoneId.of(id, ZoneId.SHORT_IDS)`, which accepts forms such as
 * `Z`, `+8`, `+08:00:00`, `GMT+8` and `PST`. Native code parses them with arrow's `Tz`, which
 * accepts only IANA zone names and offsets written as `+HH`, `+HHMM` or `+HH:MM`.
 */
object CometTimeZone {

  /**
   * The ID to pass to native code for `timeZoneId`, or None when native code cannot represent the
   * zone, which is the case for an offset with seconds. A fixed offset becomes `+HH:MM`, a zero
   * offset becomes `UTC`, and a short ID becomes its region. An expression with no timezone gets
   * `UTC`: Spark leaves it unset only on casts that do not use it.
   */
  def nativeId(timeZoneId: Option[String]): Option[String] = timeZoneId match {
    case None => Some("UTC")
    case Some(id) =>
      Try(DateTimeUtils.getZoneId(id).normalized()).toOption.flatMap {
        case offset: ZoneOffset if offset.getTotalSeconds == 0 => Some("UTC")
        case offset: ZoneOffset if offset.getTotalSeconds % 60 == 0 => Some(offset.getId)
        case _: ZoneOffset => None
        case region => Some(region.getId)
      }
  }

  /** Whether `timeZoneId` has a zero offset at every instant. */
  def isUtc(timeZoneId: Option[String]): Boolean = nativeId(timeZoneId).contains("UTC")

  /** `Unsupported` when native code cannot represent `timeZoneId`, `Compatible` otherwise. */
  def supportLevel(timeZoneId: Option[String]): SupportLevel = nativeId(timeZoneId) match {
    case Some(_) => Compatible()
    case None => Unsupported(Some(unsupportedReason(timeZoneId)))
  }

  def unsupportedReason(timeZoneId: Option[String]): String =
    s"Timezone ${timeZoneId.getOrElse("")} cannot be represented in native code"
}
