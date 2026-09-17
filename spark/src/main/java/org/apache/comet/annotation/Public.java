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

package org.apache.comet.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a type or method as part of Comet's public API, as enumerated in the <a
 * href="https://datafusion.apache.org/comet/about/versioning_policy.html">versioning policy</a>.
 *
 * <p>Public API is covered by Comet's compatibility guarantees: removing it, or changing it in a
 * way that breaks existing users, requires a deprecation cycle of at least one minor release
 * followed by removal in a major release. For the S3 credential provider SPI, which vendors compile
 * against and ship as a separate jar, this covers binary compatibility as well as source
 * compatibility.
 *
 * <p>Comet's public API is deliberately small. <b>Anything not carrying this annotation is
 * internal</b>, whatever its access modifier says, and may be renamed, changed, or removed in any
 * release including a patch release. Do not add this annotation to a type without agreeing the
 * addition on the mailing list or in an issue first, since doing so commits the project to
 * supporting it.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Public {}
