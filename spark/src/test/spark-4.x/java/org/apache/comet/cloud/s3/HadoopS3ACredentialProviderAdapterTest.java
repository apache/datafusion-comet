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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Offline test of the v2 Hadoop adapter: delegating to Hadoop's {@code SimpleAWSCredentialsProvider}
 * with static keys resolves without any network. The static keys are passed directly here (the
 * native path strips them from the forwarded map; a unit test controls the map itself).
 */
public class HadoopS3ACredentialProviderAdapterTest {

  private static CometS3Credentials resolve(Map<String, String> props, String bucket)
      throws Exception {
    HadoopS3ACredentialProviderAdapter adapter = new HadoopS3ACredentialProviderAdapter();
    try {
      adapter.initialize(props);
      return adapter.getCredentialsForPath(
          new CometS3CredentialContext(bucket, "/obj", CometS3AccessMode.READ));
    } finally {
      adapter.close();
    }
  }

  @Test
  public void delegatesToSimpleProviderWithStaticKeys() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.access.key", "AKGLOBAL");
    props.put("fs.s3a.secret.key", "SKGLOBAL");

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("AKGLOBAL", creds.getAccessKeyId());
    assertEquals("SKGLOBAL", creds.getSecretAccessKey());
  }

  @Test
  public void perBucketOverrideWins() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.access.key", "AKGLOBAL");
    props.put("fs.s3a.secret.key", "SKGLOBAL");
    props.put("fs.s3a.bucket.my-bucket.access.key", "AKBUCKET");
    props.put("fs.s3a.bucket.my-bucket.secret.key", "SKBUCKET");

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("AKBUCKET", creds.getAccessKeyId());
    assertEquals("SKBUCKET", creds.getSecretAccessKey());
  }
}
