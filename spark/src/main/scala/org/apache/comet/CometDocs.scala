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

/**
 * Builds links into the published user guide that match the running build, so that a message from
 * Comet 1.2.x points at the 1.2 guide rather than at a guide that has since moved on. A snapshot
 * build, or one whose version is unknown, points at the development guide built from `main`.
 */
object CometDocs {

  private val BaseUrl = "https://datafusion.apache.org/comet/user-guide"

  private val ReleaseVersion = """(\d+\.\d+)\.\d+""".r

  /**
   * The user guide directory that documents the given Comet version: `1.2` for `1.2.0` and
   * `1.2.3`, and `dev` for `1.2.0-SNAPSHOT`, `<unknown>` and anything else that is not a release.
   */
  def userGuideVersion(cometVersion: String): String = cometVersion match {
    case ReleaseVersion(majorMinor) => majorMinor
    case _ => "dev"
  }

  /** URL of a user guide page, for example `userGuideUrl("tuning.html")`. */
  def userGuideUrl(page: String): String =
    s"$BaseUrl/${userGuideVersion(COMET_VERSION)}/$page"
}
