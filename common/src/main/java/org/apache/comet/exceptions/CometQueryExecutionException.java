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

package org.apache.comet.exceptions;

import org.apache.comet.CometNativeException;

/**
 * Exception thrown from Comet native execution containing JSON-encoded error information. The
 * message contains a JSON object with the following structure:
 *
 * <pre>
 * {
 *   "errorType": "DivideByZero",
 *   "errorClass": "DIVIDE_BY_ZERO",
 *   "params": { ... },
 *   "context": { "sqlText": "...", "startOffset": 0, "stopOffset": 10 },
 *   "hint": "Use `try_divide` to tolerate divisor being 0"
 * }
 * </pre>
 *
 * CometExecIterator parses this JSON and converts it to the appropriate Spark exception by calling
 * the corresponding QueryExecutionErrors.* method.
 */
public final class CometQueryExecutionException extends CometNativeException {

  /**
   * Creates a new CometQueryExecutionException with a JSON-encoded error message.
   *
   * @param jsonMessage JSON string containing error information
   */
  public CometQueryExecutionException(String jsonMessage) {
    super(jsonMessage);
  }

  /**
   * Returns true if the message appears to be JSON-formatted. This is used to distinguish between
   * JSON-encoded errors and legacy error messages.
   *
   * @return true if message starts with '{' and ends with '}'
   */
  public boolean isJsonMessage() {
    String msg = getMessage();
    return msg != null && msg.trim().startsWith("{") && msg.trim().endsWith("}");
  }
}
