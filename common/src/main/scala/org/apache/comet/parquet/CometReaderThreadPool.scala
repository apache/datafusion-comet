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

package org.apache.comet.parquet

import java.util.concurrent.{Executors, ExecutorService, ThreadFactory}
import java.util.concurrent.atomic.AtomicLong

abstract class CometReaderThreadPool {
  private var threadPool: Option[ExecutorService] = None

  protected def threadNamePrefix: String

  private def initThreadPool(maxThreads: Int): ExecutorService = synchronized {
    if (threadPool.isEmpty) {
      val threadFactory: ThreadFactory = new ThreadFactory() {
        private val defaultThreadFactory = Executors.defaultThreadFactory
        val count = new AtomicLong(0)

        override def newThread(r: Runnable): Thread = {
          val thread = defaultThreadFactory.newThread(r)
          thread.setName(s"${threadNamePrefix}_${count.getAndIncrement()}")
          thread.setDaemon(true)
          thread
        }
      }

      val threadPoolExecutor = Executors.newFixedThreadPool(maxThreads, threadFactory)
      threadPool = Some(threadPoolExecutor)
    }

    threadPool.get
  }

  def getOrCreateThreadPool(numThreads: Int): ExecutorService = {
    threadPool.getOrElse(initThreadPool(numThreads))
  }

}

// A thread pool used for pre-fetching files.
object CometPrefetchThreadPool extends CometReaderThreadPool {
  override def threadNamePrefix: String = "prefetch_thread"
}

// Thread pool used by the Parquet parallel reader
object CometFileReaderThreadPool extends CometReaderThreadPool {
  override def threadNamePrefix: String = "file_reader_thread"
}
