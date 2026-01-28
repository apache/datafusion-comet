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

/**
 * JNI bridge for accessing Scala object methods as static methods.
 *
 * <p>This class provides static methods that can be called from native code via JNI, delegating to
 * the Scala Native object singleton.
 */
public class NativeJNIBridge {

  /**
   * Gets Iceberg partition tasks for the current thread (JNI-accessible static method).
   *
   * <p>This method is called by native Rust code via JNI to retrieve partition-specific tasks
   * during Iceberg scan execution.
   *
   * @return Serialized protobuf bytes containing IcebergFileScanTask messages, or null if not set
   */
  public static byte[] getIcebergPartitionTasksInternal() {
    return Native$.MODULE$.getIcebergPartitionTasksInternal();
  }
}
