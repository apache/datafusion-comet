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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** AWS SDK v2 raw-provider adapter (spark-4.x source set). */
public class AwsSdkCredentialProviderAdapterTest {

  private static final String KEY = "fs.s3a.comet.credential.adapter.class";

  private static CometS3Credentials resolve(Map<String, String> props) throws Exception {
    AwsSdkCredentialProviderAdapter adapter = new AwsSdkCredentialProviderAdapter();
    try {
      adapter.initialize(props);
      return adapter.getCredentialsForPath(
          new CometS3CredentialContext("bkt", "/obj", CometS3AccessMode.READ));
    } finally {
      adapter.close();
    }
  }

  @Test
  public void instantiatesViaNoArgConstructor() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(KEY, V2NoArgProvider.class.getName());
    assertEquals("noarg-ak", resolve(props).getAccessKeyId());
  }

  @Test
  public void createFactoryTakesPrecedenceOverConstructor() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(KEY, V2CreateProvider.class.getName());
    assertEquals("create-ak", resolve(props).getAccessKeyId());
  }

  @Test
  public void builderFactoryWithPrivateImplWorks() throws Exception {
    // The builder() returns a public interface backed by a private impl; the adapter must resolve
    // build() off the public declared type, not the private runtime class.
    Map<String, String> props = new HashMap<>();
    props.put(KEY, V2BuilderProvider.class.getName());
    assertEquals("builder-ak", resolve(props).getAccessKeyId());
  }

  @Test
  public void builderWithErasedBuildReturnTypeWorks() throws Exception {
    // build() is inherited from a generic super-interface, so reflection reports Object; the
    // adapter must accept it by the built instance's runtime type.
    Map<String, String> props = new HashMap<>();
    props.put(KEY, V2GenericBuilderProvider.class.getName());
    assertEquals("generic-builder-ak", resolve(props).getAccessKeyId());
  }

  @Test
  public void unrelatedCreateFactoryIsSkipped() throws Exception {
    // A static create() whose return type is not a provider must be ignored (return-type check),
    // falling through to the no-arg constructor instead of failing with a ClassCastException.
    Map<String, String> props = new HashMap<>();
    props.put(KEY, V2UnrelatedCreateProvider.class.getName());
    assertEquals("noarg-ak", resolve(props).getAccessKeyId());
  }

  @Test
  public void missingDelegateClassThrows() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> resolve(new HashMap<>()));
    assertTrue(e.getMessage().contains("comet.credential.adapter.class"));
  }

  @Test
  public void unknownDelegateClassThrows() {
    Map<String, String> props = new HashMap<>();
    props.put(KEY, "com.example.DoesNotExist");
    assertThrows(ClassNotFoundException.class, () -> resolve(props));
  }

  @Test
  public void wrongTypeDelegateThrows() {
    Map<String, String> props = new HashMap<>();
    props.put(KEY, NotACredentialProvider.class.getName());
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> resolve(props));
    assertTrue(e.getMessage().contains("does not implement"));
  }

  @Test
  public void usesLoaderCapturedAtInitializeOnNullContextThread() throws Exception {
    // initialize() captures the context loader; a later fetch on a null-context thread (as native
    // worker threads are) must load the delegate with that captured loader, not the current TCCL.
    String target = V2NoArgProvider.class.getName();
    CapturingClassLoaderSupport.RecordingClassLoader recording =
        new CapturingClassLoaderSupport.RecordingClassLoader(target, getClass().getClassLoader());
    AwsSdkCredentialProviderAdapter adapter = new AwsSdkCredentialProviderAdapter();
    Map<String, String> props = new HashMap<>();
    props.put(KEY, target);
    try {
      CapturingClassLoaderSupport.onThread(
          recording,
          () -> {
            adapter.initialize(props);
            return null;
          });
      CometS3Credentials creds =
          CapturingClassLoaderSupport.onThread(
              null,
              () ->
                  adapter.getCredentialsForPath(
                      new CometS3CredentialContext("bkt", "/obj", CometS3AccessMode.READ)));
      assertEquals("noarg-ak", creds.getAccessKeyId());
      assertTrue(
          "the captured loader should have loaded the delegate", recording.loaded.contains(target));
    } finally {
      adapter.close();
    }
  }
}
