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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.Map;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

/** Config and reflection helpers shared by the built-in S3 credential provider adapters. */
final class AdapterSupport {

  private AdapterSupport() {}

  /**
   * Rebuilds a Hadoop {@link Configuration} for the adapter to hand to the S3A provider factory.
   * Seeds from the executor's own Spark-derived Hadoop conf (so keys the provider reads that Comet
   * does not forward -- e.g. {@code hadoop.security.credential.provider.path} set via {@code
   * spark.hadoop.*}, which is not an {@code fs.s3a.*} key -- are present), then overlays the
   * forwarded {@code fs.s3a.*} map on top. Off the executor (e.g. in unit tests) there is no {@code
   * SparkEnv}, so it falls back to a bare Configuration that still loads {@code core-site.xml}.
   */
  static Configuration toConfiguration(Map<String, String> props) {
    Configuration conf = new Configuration();
    SparkEnv env = SparkEnv.get();
    if (env != null) {
      SparkConf sparkConf = env.conf();
      // spark.hadoop.<k>=<v> maps to Hadoop conf key <k>, matching SparkHadoopUtil.
      for (Tuple2<String, String> kv : sparkConf.getAllWithPrefix("spark.hadoop.")) {
        conf.set(kv._1(), kv._2());
      }
    }
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
  private static Method staticMethod(Class<?> clazz, String name) {
    try {
      Method m = clazz.getMethod(name);
      return Modifier.isStatic(m.getModifiers()) ? m : null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /**
   * Instantiates a credential-provider delegate, trying the same ordered conventions for both the
   * v1 and v2 adapters so their {@code @Public} contract is identical: the Hadoop-style {@code
   * (URI, Configuration)} and {@code (Configuration)} constructors first (matching {@code
   * S3AUtils.getInstanceFromReflection}), then the SDK static factories {@code create()} / {@code
   * builder().build()} / {@code getInstance()}, then a public no-arg constructor.
   *
   * <p>Factory return types must be assignable to {@code targetType} (as Hadoop's {@code
   * getFactoryMethod} requires), so an unrelated {@code static String create()} is skipped rather
   * than invoked and failing later with a {@code ClassCastException}. Returns an untyped instance;
   * the caller casts to its SDK provider interface.
   */
  static Object instantiateDelegate(
      Class<?> targetType, Class<?> clazz, URI uri, Configuration conf) throws Exception {
    Constructor<?> uriConf = constructor(clazz, URI.class, Configuration.class);
    if (uriConf != null) {
      return uriConf.newInstance(uri, conf);
    }
    Constructor<?> confOnly = constructor(clazz, Configuration.class);
    if (confOnly != null) {
      return confOnly.newInstance(conf);
    }
    Method create = factoryMethod(clazz, "create", targetType);
    if (create != null) {
      return create.invoke(null);
    }
    Method builder = staticMethod(clazz, "builder");
    if (builder != null) {
      // build()'s declared return type may be erased to Object (a public Builder that inherits
      // build() from a generic SdkBuilder<B, T> without redeclaring it), so check the built
      // instance's runtime type rather than the declared return type. Fall through if the builder
      // does not yield the target type.
      Object built = tryBuild(builder);
      if (targetType.isInstance(built)) {
        return built;
      }
    }
    Method getInstance = factoryMethod(clazz, "getInstance", targetType);
    if (getInstance != null) {
      return getInstance.invoke(null);
    }
    return clazz.getDeclaredConstructor().newInstance();
  }

  /**
   * Invokes {@code builder().build()} and returns the built object, or null if there is no public
   * no-arg {@code build()} or the builder yields null. Resolves {@code build()} off {@code
   * builder()}'s declared (public) return type, not the runtime object's class, which may be a
   * non-public implementation.
   */
  private static Object tryBuild(Method builder) throws Exception {
    Object b = builder.invoke(null);
    if (b == null) {
      return null;
    }
    Method build;
    try {
      build = builder.getReturnType().getMethod("build");
    } catch (NoSuchMethodException e) {
      return null;
    }
    return build.invoke(b);
  }

  private static Constructor<?> constructor(Class<?> clazz, Class<?>... params) {
    try {
      return clazz.getConstructor(params);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /** A public static no-arg factory whose return type is assignable to {@code targetType}. */
  private static Method factoryMethod(Class<?> clazz, String name, Class<?> targetType) {
    Method m = staticMethod(clazz, name);
    return (m != null && targetType.isAssignableFrom(m.getReturnType())) ? m : null;
  }

  /**
   * Replicates the step {@code S3AFileSystem.initialize} performs before building the provider
   * list: promote the S3A credential-store path ({@code fs.s3a.security.credential.provider.path})
   * into Hadoop's generic {@code hadoop.security.credential.provider.path}, so a provider that
   * looks up a secret through Hadoop's credential-provider API can see the configured store. The
   * factory methods the adapters call do not do this on their own. The S3A path takes precedence
   * over any generic path already set. Call after {@code propagateBucketOptions} so per-bucket
   * store paths are already promoted to the base key.
   */
  static void patchSecurityCredentialProviders(Configuration conf) {
    String s3aPath = conf.getTrimmed("fs.s3a.security.credential.provider.path");
    if (s3aPath == null || s3aPath.isEmpty()) {
      return;
    }
    String generic = conf.getTrimmed("hadoop.security.credential.provider.path");
    String merged = (generic == null || generic.isEmpty()) ? s3aPath : s3aPath + "," + generic;
    conf.set("hadoop.security.credential.provider.path", merged);
  }

  /**
   * Fails if S3A delegation tokens are configured. {@code S3AFileSystem.initialize} switches to the
   * delegation-token provider and bypasses the configured credential-provider chain; the Hadoop
   * adapter always builds the chain, so on a DT cluster it would resolve a different identity than
   * Spark. Rather than silently do that, refuse. Call after {@code propagateBucketOptions} so a
   * per-bucket binding is already promoted to the base key.
   */
  static void checkNoDelegationTokenBinding(Configuration conf) {
    String binding = conf.getTrimmed("fs.s3a.delegation.token.binding");
    if (binding != null && !binding.isEmpty()) {
      throw new IllegalStateException(
          "fs.s3a.delegation.token.binding="
              + binding
              + " is configured. Spark uses the delegation-token provider and bypasses the"
              + " credential-provider chain, but this adapter would resolve the configured chain"
              + " instead (a different identity). S3A delegation tokens are not supported by the"
              + " Comet S3 credential adapters.");
    }
  }
}
