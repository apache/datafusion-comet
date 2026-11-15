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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Test fixture: a v2 provider whose public {@code Builder} inherits {@code build()} from a generic
 * super-interface without redeclaring it, so reflection reports the erased {@code Object} return
 * type. The adapter must accept it by the built instance's runtime type, not the declared return
 * type (mirrors AWS SDK v2's {@code SdkBuilder<B, T>} shape).
 */
public class V2GenericBuilderProvider implements AwsCredentialsProvider {

  private V2GenericBuilderProvider() {}

  public static Builder builder() {
    return new BuilderImpl();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return AwsBasicCredentials.create("generic-builder-ak", "sk");
  }

  /** Generic builder super-interface; {@code build()} erases to {@code Object}. */
  public interface GenericBuilder<B, T> {
    T build();
  }

  /** Public builder type that only inherits {@code build()} — it does not redeclare it. */
  public interface Builder extends GenericBuilder<Builder, V2GenericBuilderProvider> {}

  private static final class BuilderImpl implements Builder {
    @Override
    public V2GenericBuilderProvider build() {
      return new V2GenericBuilderProvider();
    }
  }
}
