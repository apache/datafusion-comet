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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Offline test of the v2 Hadoop adapter: delegating to Hadoop's {@code
 * SimpleAWSCredentialsProvider} with static keys resolves without any network. The unit test
 * supplies the {@code fs.s3a.*} map directly (the end-to-end forwarding is covered by the MinIO
 * bridge suite). The profile tests point Hadoop's {@code ProfileAWSCredentialsProvider} at
 * temporary credentials files, so they never read the developer's own AWS files, and are skipped on
 * hadoop-aws releases older than 3.4.2.
 */
public class HadoopS3ACredentialProviderAdapterTest {

  private static final String PROFILE_PROVIDER =
      "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider";

  private final List<Path> tempFiles = new ArrayList<>();

  @After
  public void deleteTempFiles() throws Exception {
    for (Path file : tempFiles) {
      Files.deleteIfExists(file);
    }
    tempFiles.clear();
  }

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

  /** Hadoop's profile provider first shipped in hadoop-aws 3.4.2. */
  private static void assumeProfileProviderAvailable() {
    boolean available;
    try {
      Class.forName(PROFILE_PROVIDER);
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    assumeTrue("needs hadoop-aws 3.4.2 or later", available);
  }

  /** Writes an owner-only temporary file, deleted after the test, and returns its path. */
  private String writeTempFile(String content) throws Exception {
    Path file = Files.createTempFile("comet-credentials", ".ini");
    tempFiles.add(file);
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    return file.toAbsolutePath().toString();
  }

  /** A credentials file with a default profile and an {@code analytics} profile. */
  private String writeCredentialsFile() throws Exception {
    return writeTempFile(
        "[default]\n"
            + "aws_access_key_id = AKDEFAULT\n"
            + "aws_secret_access_key = SKDEFAULT\n"
            + "[analytics]\n"
            + "aws_access_key_id = AKPROFILE\n"
            + "aws_secret_access_key = SKPROFILE\n");
  }

  @Test
  public void delegatesToSimpleProviderWithStaticKeys() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
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
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.access.key", "AKGLOBAL");
    props.put("fs.s3a.secret.key", "SKGLOBAL");
    props.put("fs.s3a.bucket.my-bucket.access.key", "AKBUCKET");
    props.put("fs.s3a.bucket.my-bucket.secret.key", "SKBUCKET");

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("AKBUCKET", creds.getAccessKeyId());
    assertEquals("SKBUCKET", creds.getSecretAccessKey());
  }

  @Test
  public void resolvesKeysFromCredentialStoreViaS3aProviderPath() throws Exception {
    // Secrets live only in a Hadoop credential store (a jceks file), not in fs.s3a.access.key. The
    // adapter must promote fs.s3a.security.credential.provider.path into the generic
    // hadoop.security.credential.provider.path (as S3AFileSystem.initialize does) so
    // SimpleAWSCredentialsProvider's conf.getPassword lookup finds them.
    File dir = Files.createTempDirectory("comet-creds").toFile();
    dir.deleteOnExit();
    String jceks = "jceks://file" + new File(dir, "s3a.jceks").getAbsolutePath();

    Configuration provConf = new Configuration();
    provConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceks);
    CredentialProvider store = CredentialProviderFactory.getProviders(provConf).get(0);
    store.createCredentialEntry("fs.s3a.access.key", "STOREAK".toCharArray());
    store.createCredentialEntry("fs.s3a.secret.key", "STORESK".toCharArray());
    store.flush();

    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.security.credential.provider.path", jceks);

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("STOREAK", creds.getAccessKeyId());
    assertEquals("STORESK", creds.getSecretAccessKey());
  }

  @Test
  public void resolvesPerBucketWithinOneInstance() throws Exception {
    // One instance can serve multiple buckets (the Iceberg dispatch key is the catalog, not the
    // bucket), so the delegate must be cached per bucket, not once.
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.bucket.bkt-a.access.key", "AK-A");
    props.put("fs.s3a.bucket.bkt-a.secret.key", "SK-A");
    props.put("fs.s3a.bucket.bkt-b.access.key", "AK-B");
    props.put("fs.s3a.bucket.bkt-b.secret.key", "SK-B");

    HadoopS3ACredentialProviderAdapter adapter = new HadoopS3ACredentialProviderAdapter();
    try {
      adapter.initialize(props);
      CometS3Credentials a =
          adapter.getCredentialsForPath(
              new CometS3CredentialContext("bkt-a", "/o", CometS3AccessMode.READ));
      CometS3Credentials b =
          adapter.getCredentialsForPath(
              new CometS3CredentialContext("bkt-b", "/o", CometS3AccessMode.READ));
      assertEquals("AK-A", a.getAccessKeyId());
      assertEquals("AK-B", b.getAccessKeyId());
    } finally {
      adapter.close();
    }
  }

  @Test
  public void refusesDelegationTokenBinding() {
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.access.key", "AK");
    props.put("fs.s3a.secret.key", "SK");
    // Per-bucket spelling also pins that the check runs after propagateBucketOptions promotes it.
    props.put(
        "fs.s3a.bucket.my-bucket.delegation.token.binding",
        "org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding");

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> resolve(props, "my-bucket"));
    assertTrue(e.getMessage().contains("fs.s3a.delegation.token.binding"));
  }

  @Test
  public void refusesAnonymousCredentials() {
    // A public-dataset bucket configured with AnonymousAWSCredentialsProvider resolves credentials
    // with null keys. The adapter must fail with a clear cause naming the bucket, not an opaque
    // NullPointerException, since the SPI cannot express anonymous access.
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> resolve(props, "public-data"));
    assertTrue(e.getMessage().contains("anonymous"));
    assertTrue(e.getMessage().contains("public-data"));
  }

  @Test
  public void resolvesNamedProfileFromConfiguredFile() throws Exception {
    // Comet forwards the per-bucket profile keys as written; the adapter promotes them the way
    // S3AFileSystem does, so Hadoop's provider reads the named profile from the named file. The
    // global file holds decoy keys under the same profile name, so the per-bucket file must win.
    assumeProfileProviderAvailable();
    String decoyFile =
        writeTempFile(
            "[analytics]\n"
                + "aws_access_key_id = AKDECOY\n"
                + "aws_secret_access_key = SKDECOY\n");
    Map<String, String> props = new HashMap<>();
    props.put("fs.s3a.aws.credentials.provider", PROFILE_PROVIDER);
    props.put("fs.s3a.auth.profile.file", decoyFile);
    props.put("fs.s3a.bucket.my-bucket.auth.profile.file", writeCredentialsFile());
    props.put("fs.s3a.bucket.my-bucket.auth.profile.name", "analytics");

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("AKPROFILE", creds.getAccessKeyId());
    assertEquals("SKPROFILE", creds.getSecretAccessKey());
    assertEquals(0L, creds.getExpirationEpochMillis());
  }

  @Test
  public void profileProviderFallsThroughHadoopList() throws Exception {
    // A profile missing from the file fails that entry, and Hadoop's list moves on to the next.
    assumeProfileProviderAvailable();
    Map<String, String> props = new HashMap<>();
    props.put(
        "fs.s3a.aws.credentials.provider",
        PROFILE_PROVIDER + ",org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    props.put("fs.s3a.auth.profile.file", writeCredentialsFile());
    props.put("fs.s3a.auth.profile.name", "missing");
    props.put("fs.s3a.access.key", "AKSTATIC");
    props.put("fs.s3a.secret.key", "SKSTATIC");

    CometS3Credentials creds = resolve(props, "my-bucket");
    assertEquals("AKSTATIC", creds.getAccessKeyId());
    assertEquals("SKSTATIC", creds.getSecretAccessKey());
  }
}
