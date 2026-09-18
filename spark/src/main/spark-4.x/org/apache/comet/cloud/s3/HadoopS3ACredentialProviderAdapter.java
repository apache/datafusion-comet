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

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.comet.annotation.Public;

/**
 * Delegates credential resolution to Hadoop S3A's own provider construction, so it accepts
 * everything the {@code fs.s3a.aws.credentials.provider} chain accepts. This is the spark-4.x (AWS
 * SDK v2) body; it calls {@link CredentialProviderListFactory} and returns v2 credentials.
 *
 * <p>Enable it (leaving {@code fs.s3a.aws.credentials.provider} untouched) with:
 *
 * <pre>
 * spark.hadoop.fs.s3a.comet.credential.provider.class=org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapter
 * </pre>
 */
@Public
public class HadoopS3ACredentialProviderAdapter implements CometS3CredentialProvider {

  private Map<String, String> properties;
  private volatile AwsCredentialsProvider delegate;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context)
      throws Exception {
    AwsCredentialsProvider provider = ensureDelegate(context.getBucket());
    return SdkCredentialExtraction.toCometCredentials(provider.resolveCredentials());
  }

  private AwsCredentialsProvider ensureDelegate(String bucket) throws Exception {
    AwsCredentialsProvider local = delegate;
    if (local != null) {
      return local;
    }
    synchronized (this) {
      if (delegate == null) {
        Configuration conf =
            S3AUtils.propagateBucketOptions(AdapterSupport.toConfiguration(properties), bucket);
        URI uri = new URI("s3a://" + bucket + "/");
        delegate = CredentialProviderListFactory.createAWSCredentialProviderList(uri, conf);
      }
      return delegate;
    }
  }

  @Override
  public void close() throws Exception {
    AwsCredentialsProvider local = delegate;
    if (local instanceof AutoCloseable) {
      ((AutoCloseable) local).close();
    }
  }
}
