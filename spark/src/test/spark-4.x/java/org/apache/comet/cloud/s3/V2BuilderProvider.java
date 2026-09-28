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
 * Test fixture: a v2 provider built via a static {@code builder()} that returns a public builder
 * interface backed by a private implementation class. This mirrors the SDK convention and
 * reproduces the case where resolving {@code build()} off the runtime (private) class would throw
 * {@code IllegalAccessException}.
 */
public class V2BuilderProvider implements AwsCredentialsProvider {

  private V2BuilderProvider() {}

  public static Builder builder() {
    return new BuilderImpl();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return AwsBasicCredentials.create("builder-ak", "sk");
  }

  /** Public builder type; {@code build()} is declared here. */
  public interface Builder {
    V2BuilderProvider build();
  }

  private static final class BuilderImpl implements Builder {
    @Override
    public V2BuilderProvider build() {
      return new V2BuilderProvider();
    }
  }
}
