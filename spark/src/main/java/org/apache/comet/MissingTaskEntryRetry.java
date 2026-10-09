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

package org.apache.comet;

import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;

/**
 * Retries a request for execution memory that Spark failed because the task's entry was gone from
 * its execution pool.
 *
 * <p>Spark registers a task's entry in {@code ExecutionMemoryPool} once, before its wait loop, and
 * removes it when the task's balance reaches zero. A request that waits for memory below the task's
 * minimum share then fails when it wakes up with a NoSuchElementException ("key not found: " and
 * the task id). Releases take no task monitor, so any other consumer of the same task, JVM or
 * native, can empty the balance while a request waits. The failed call was granted nothing, because
 * Spark only drops the entry at a zero balance, which cannot hold while the call has a partial
 * grant. So trying again is safe: it registers the task again and waits for its share as Spark
 * would have. Nothing here holds a lock across attempts, so a retry parks in Spark exactly as the
 * first attempt did. The Spark issue is SPARK-59444.
 */
public final class MissingTaskEntryRetry {

  /** Attempts at a request whose task entry Spark keeps losing. */
  public static final int MAX_ATTEMPTS = 3;

  private MissingTaskEntryRetry() {}

  /**
   * Runs {@code request}, a call that asks Spark for {@code required} bytes for task {@code
   * taskAttemptId}, and retries it when it lost that task's entry. See {@link #retry(Logger, long,
   * Supplier, Function)}.
   */
  public static <T> T retry(
      Logger logger,
      long taskAttemptId,
      long required,
      Supplier<T> request,
      Function<NoSuchElementException, T> refuse) {
    return retry(logger, OptionalLong.of(taskAttemptId), required, request, refuse);
  }

  /**
   * Runs {@code request}, a call that asks Spark for {@code required} bytes, and retries it when it
   * lost the task's entry. Without the task id, any task's missing entry counts. Other exceptions
   * are rethrown. After {@link #MAX_ATTEMPTS} attempts that all lost the entry, returns what {@code
   * refuse} makes of the last attempt's exception, or throws what it throws. Both the retries and
   * the refusal are logged to {@code logger}.
   */
  public static <T> T retry(
      Logger logger,
      long required,
      Supplier<T> request,
      Function<NoSuchElementException, T> refuse) {
    return retry(logger, OptionalLong.empty(), required, request, refuse);
  }

  private static <T> T retry(
      Logger logger,
      OptionalLong taskAttemptId,
      long required,
      Supplier<T> request,
      Function<NoSuchElementException, T> refuse) {
    for (int attempt = 1; ; attempt++) {
      try {
        return request.get();
      } catch (NoSuchElementException e) {
        if (!isMissingTaskEntry(e, taskAttemptId)) {
          throw e;
        }
        String task = taskAttemptId.isPresent() ? "Task " + taskAttemptId.getAsLong() : "The task";
        if (attempt >= MAX_ATTEMPTS) {
          logger.warn(
              "{} lost its execution memory entry in Spark on {} attempts to acquire {} bytes, "
                  + "refusing the request",
              task,
              attempt,
              required,
              e);
          return refuse.apply(e);
        }
        logger.info(
            "{} lost its execution memory entry in Spark while waiting to acquire {} bytes, "
                + "trying again",
            task,
            required);
      }
    }
  }

  /**
   * Whether {@code e} is Spark's execution pool failing to find the task's entry: the message names
   * the task, or any task when its id is unknown, and the exception comes from the pool.
   */
  static boolean isMissingTaskEntry(NoSuchElementException e, OptionalLong taskAttemptId) {
    String message = e.getMessage();
    boolean matches =
        taskAttemptId.isPresent()
            ? ("key not found: " + taskAttemptId.getAsLong()).equals(message)
            : message != null && message.startsWith("key not found: ");
    if (!matches) {
      return false;
    }
    for (StackTraceElement frame : e.getStackTrace()) {
      if ("org.apache.spark.memory.ExecutionMemoryPool".equals(frame.getClassName())) {
        return true;
      }
    }
    return false;
  }
}
