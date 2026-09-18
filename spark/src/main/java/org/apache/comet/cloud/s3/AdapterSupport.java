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

package org.apache.comet.cloud.s3;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/** Config and reflection helpers shared by the built-in S3 credential provider adapters. */
final class AdapterSupport {

  private AdapterSupport() {}

  /**
   * Rebuilds a Hadoop {@link Configuration} from the forwarded {@code fs.s3a.*} map. The adapters
   * run on the executor without a live {@code S3AFileSystem}, so keys are copied onto a fresh
   * Configuration (which still loads core-site defaults).
   */
  static Configuration toConfiguration(Map<String, String> props) {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (entry.getValue() != null) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }

  /**
   * Per-bucket then global lookup, mirroring Comet's native {@code fs.s3a} resolution: {@code
   * fs.s3a.bucket.<bucket>.<property>} wins over {@code fs.s3a.<property>}. Returns null if neither
   * is set (after trimming).
   */
  static String lookup(Map<String, String> props, String bucket, String property) {
    String perBucket = props.get("fs.s3a.bucket." + bucket + "." + property);
    if (perBucket != null && !perBucket.trim().isEmpty()) {
      return perBucket.trim();
    }
    String global = props.get("fs.s3a." + property);
    if (global != null && !global.trim().isEmpty()) {
      return global.trim();
    }
    return null;
  }

  /** Returns the public static no-arg method {@code name} on {@code clazz}, or null if absent. */
  static Method staticMethod(Class<?> clazz, String name) {
    try {
      Method m = clazz.getMethod(name);
      return Modifier.isStatic(m.getModifiers()) ? m : null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
