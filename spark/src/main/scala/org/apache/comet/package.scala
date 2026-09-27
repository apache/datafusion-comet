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

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.internal.Logging

package object comet {

  /**
   * The root allocator for Comet execution. Because Arrow Java memory management is based on
   * reference counting, exposed arrays increase the reference count of the underlying buffers.
   * Until the reference count is zero, the memory will not be released. If the consumer side is
   * finished later than the close of the allocator, the allocator will think the memory is
   * leaked. To avoid this, we use a single allocator for the whole execution process.
   */
  val CometArrowAllocator = new RootAllocator(Long.MaxValue)

  /**
   * The allocator that the Arrow C Data Interface import path allocates from.
   *
   * Arrow charges a buffer to whichever allocator owns it, so imports taken directly against
   * [[CometArrowAllocator]] are indistinguishable from buffers the JVM allocated itself. Giving
   * the import path its own child keeps the two separable for tracing. The child reserves
   * nothing, so every byte still escalates to the parent and the root keeps reporting the total.
   * Like the root, it is never closed: imported buffers are reference counted and routinely
   * outlive the task that imported them.
   *
   * What this counts is what the import path is charged for, not where the bytes were allocated.
   * Ownership and allocation come apart in both directions. Bytes the JVM allocated land here:
   * Arrow's importer allocates the owning `ArrowArray` struct from this allocator, and
   * `BitVectorHelper.loadValidityBuffer` allocates a validity bitmap here when an imported vector
   * is all-valid or all-null and carries no validity buffer. Imported bytes land elsewhere: an
   * ownership transfer re-parents a charge without moving the payload, so a vector that shares
   * buffers with an import can leave the root accountable for memory the producer allocated.
   *
   * So read this and the root's total as allocator charges. Their difference is not a bound on
   * the Arrow memory the JVM allocated itself, and neither is a count of unique physical bytes.
   */
  val CometArrowImportAllocator: BufferAllocator =
    CometArrowAllocator.newChildAllocator("comet-ffi-imports", 0, Long.MaxValue)

  /**
   * Provides access to build information about the Comet libraries. This will be used by the
   * benchmarking software to provide the source revision and repository. In addition, the build
   * information is included to aid in future debugging efforts for releases.
   */
  private object CometBuildInfo extends Logging {
    private val GIT_INFO_PROPS_FILENAME = "comet-git-info.properties"

    val props: Properties = {
      val props = new Properties()
      val resourceStream = Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream(GIT_INFO_PROPS_FILENAME)
      if (resourceStream != null) {
        try {
          props.load(resourceStream)
        } catch {
          case e: Exception =>
            logError(s"Error loading properties from $GIT_INFO_PROPS_FILENAME", e)
        } finally {
          if (resourceStream != null) {
            try {
              resourceStream.close()
            } catch {
              case e: Exception =>
                logError("Error closing Comet build info resource stream", e)
            }
          }
        }
      } else {
        logWarning(s"Could not find $GIT_INFO_PROPS_FILENAME")
      }
      props
    }
  }

  private def getProp(name: String): String = {
    CometBuildInfo.props.getProperty(name, "<unknown>")
  }

  val COMET_VERSION: String = getProp("git.build.version")
  val COMET_BRANCH: String = getProp("git.branch")
  val COMET_REVISION: String = getProp("git.commit.id.full")
  val COMET_BUILD_USER_EMAIL: String = getProp("git.build.user.name")
  val COMET_BUILD_USER_NAME: String = getProp("git.build.user.email")
  val COMET_REPO_URL: String = getProp("git.remote.origin.url")
  val COMET_BUILD_TIMESTAMP: String = getProp("git.build.time")
}
