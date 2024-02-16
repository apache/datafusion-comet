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

package org.apache.spark.sql.comet.execution.shuffle;

import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.comet.CometConf$;

public class ShuffleThreadPool {
  private static ThreadPoolExecutor INSTANCE;

  /** Get the thread pool instance for shuffle writer. */
  public static synchronized ExecutorService getThreadPool() {
    if (INSTANCE == null) {
      boolean isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();

      if (isAsync) {
        ThreadFactory factory =
            new ThreadFactoryBuilder().setNameFormat("async-shuffle-writer-%d").build();

        int threadNum = (int) CometConf$.MODULE$.COMET_EXEC_SHUFFLE_ASYNC_MAX_THREAD_NUM().get();
        INSTANCE =
            new ThreadPoolExecutor(
                0, threadNum, 1L, TimeUnit.SECONDS, new ThreadPoolQueue(threadNum), factory);
      }
    }

    return INSTANCE;
  }
}

/**
 * A blocking queue for thread pool. This will block new task submission until there is space in the
 * queue.
 */
final class ThreadPoolQueue extends ArrayBlockingQueue<Runnable> {
  public ThreadPoolQueue(int capacity) {
    super(capacity);
  }

  @Override
  public boolean offer(Runnable e) {
    try {
      put(e);
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }
}
