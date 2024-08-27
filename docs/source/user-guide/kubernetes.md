<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Comet Kubernetes Support

## Comet Docker Images

Run the following command from the root of this repository to build the Comet Docker image, or use a published
Docker image from https://github.com/orgs/apache/packages?repo_name=datafusion-comet

```shell
docker build -t apache/datafusion-comet -f kube/Dockerfile .
```

## Example Spark Submit

The exact syntax will vary depending on the Kubernetes distribution, but an example `spark-submit` command can be
found [here](https://github.com/apache/datafusion-comet/tree/main/benchmarks).

