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

package org.apache

import java.util.Properties

import org.apache.arrow.memory.RootAllocator

package object comet {
  val CometArrowAllocator = new RootAllocator(Long.MaxValue)

  /**
   * Provides access to build information about the Comet libraries. This will be used by the
   * benchmarking software to provide the source revision and repository. In addition, the build
   * information is included to aid in future debugging efforts for releases.
   */
  private object CometBuildInfo {

    val (
      cometVersion: String,
      cometBranch: String,
      cometRevision: String,
      cometBuildUserName: String,
      cometBuildUserEmail: String,
      cometRepoUrl: String,
      cometBuildTimestamp: String) = {
      val resourceStream = Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream("comet-git-info.properties")
      if (resourceStream == null) {
        throw new CometRuntimeException("Could not find comet-git-info.properties")
      }

      try {
        val unknownProp = "<unknown>"
        val props = new Properties()
        props.load(resourceStream)
        (
          props.getProperty("git.build.version", unknownProp),
          props.getProperty("git.branch", unknownProp),
          props.getProperty("git.commit.id.full", unknownProp),
          props.getProperty("git.build.user.name", unknownProp),
          props.getProperty("git.build.user.email", unknownProp),
          props.getProperty("git.remote.origin.url", unknownProp),
          props.getProperty("git.build.time", unknownProp))
      } catch {
        case e: Exception =>
          throw new CometRuntimeException(
            "Error loading properties from comet-git-info.properties",
            e)
      } finally {
        if (resourceStream != null) {
          try {
            resourceStream.close()
          } catch {
            case e: Exception =>
              throw new CometRuntimeException("Error closing Comet build info resource stream", e)
          }
        }
      }
    }
  }

  val COMET_VERSION = CometBuildInfo.cometVersion
  val COMET_BRANCH = CometBuildInfo.cometBranch
  val COMET_REVISION = CometBuildInfo.cometRevision
  val COMET_BUILD_USER_EMAIL = CometBuildInfo.cometBuildUserEmail
  val COMET_BUILD_USER_NAME = CometBuildInfo.cometBuildUserName
  val COMET_REPO_URL = CometBuildInfo.cometRepoUrl
  val COMET_BUILD_TIMESTAMP = CometBuildInfo.cometBuildTimestamp

}
