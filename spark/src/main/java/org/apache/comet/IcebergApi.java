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

package org.apache.comet;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated element is part of the public API used by Apache Iceberg.
 *
 * <p>This annotation marks classes, methods, constructors, and fields that form the contract
 * between Comet and Iceberg. Changes to these APIs may break Iceberg's Comet integration, so
 * contributors should exercise caution and consider backward compatibility when modifying annotated
 * elements.
 *
 * <p>The Iceberg integration uses Comet's native Parquet reader for accelerated vectorized reads.
 * See the contributor guide documentation for details on how Iceberg uses these APIs.
 *
 * @see <a href="https://iceberg.apache.org/">Apache Iceberg</a>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
public @interface IcebergApi {}
