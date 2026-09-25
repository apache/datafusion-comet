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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;

import org.apache.comet.util.ClassLoaders;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

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
public class HadoopS3ACredentialProviderAdapter implements CometS3CredentialProvider {

  private Map<String, String> properties;
  // Captured on the thread that runs initialize() (the dispatcher calls it during planning, which
  // has Spark's user-jar loader); native worker threads have a null context loader. Handed to the
  // Configuration so S3A's factory loads the named provider classes from the right loader.
  private volatile ClassLoader classLoader;
  // One delegate per bucket: on the Iceberg path the dispatch key is the catalog, so a single
  // instance can serve multiple buckets; the Parquet path is per-bucket and uses a single entry.
  private final ConcurrentHashMap<String, AwsCredentialsProvider> delegates =
      new ConcurrentHashMap<>();

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
    this.classLoader = ClassLoaders.contextOrDefault(getClass().getClassLoader());
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context)
      throws Exception {
    AwsCredentialsProvider provider = ensureDelegate(context.getBucket());
    return SdkCredentialExtraction.toCometCredentials(provider.resolveCredentials());
  }

  private AwsCredentialsProvider ensureDelegate(String bucket) throws Exception {
    AwsCredentialsProvider existing = delegates.get(bucket);
    if (existing != null) {
      return existing;
    }
    synchronized (this) {
      AwsCredentialsProvider delegate = delegates.get(bucket);
      if (delegate == null) {
        delegate = buildDelegate(bucket);
        delegates.put(bucket, delegate);
      }
      return delegate;
    }
  }

  private AwsCredentialsProvider buildDelegate(String bucket) throws Exception {
    Configuration conf =
        S3AUtils.propagateBucketOptions(AdapterSupport.toConfiguration(properties), bucket);
    if (classLoader != null) {
      // So S3AUtils' factory loads the named provider classes from Spark's user-jar loader, not the
      // (possibly null) context loader of the native worker thread.
      conf.setClassLoader(classLoader);
    }
    AdapterSupport.patchSecurityCredentialProviders(conf);
    AdapterSupport.checkNoDelegationTokenBinding(conf);
    URI uri = new URI("s3a://" + bucket + "/");
    try {
      return CredentialProviderListFactory.createAWSCredentialProviderList(uri, conf);
    } catch (LinkageError e) {
      // This body targets AWS SDK v2 (Hadoop 3.4+). If the cluster runs Hadoop 3.3.x (SDK v1) the
      // factory descriptor differs and this surfaces as an opaque NoSuchMethodError; name it.
      throw new IllegalStateException(
          "Failed to build the S3A credential provider list. This Comet build targets AWS SDK v2"
              + " (Hadoop 3.4+); if the cluster runs Hadoop 3.3.x (SDK v1), use a Comet build for"
              + " that Spark/Hadoop line.",
          e);
    }
  }

  @Override
  public void close() throws Exception {
    for (AwsCredentialsProvider delegate : delegates.values()) {
      if (delegate instanceof AutoCloseable) {
        ((AutoCloseable) delegate).close();
      }
    }
  }
}
