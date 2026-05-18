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

package org.apache.comet.parquet;

import org.junit.Assert;
import org.junit.Test;

public class TestCometInputFile {
  @Test
  public void testIsAtLeastHadoop33() {
    Assert.assertTrue(CometInputFile.isAtLeastHadoop33("3.3.0"));
    Assert.assertTrue(CometInputFile.isAtLeastHadoop33("3.4.0-SNAPSHOT"));
    Assert.assertTrue(CometInputFile.isAtLeastHadoop33("3.12.5"));
    Assert.assertTrue(CometInputFile.isAtLeastHadoop33("3.20.6.4-xyz"));

    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("2.7.2"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("2.7.3-SNAPSHOT"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("2.7"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("2"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3.2"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3.0.2.5-abc"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3.1.2-test"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3-SNAPSHOT"));
    Assert.assertFalse(CometInputFile.isAtLeastHadoop33("3.2-SNAPSHOT"));
  }
}
