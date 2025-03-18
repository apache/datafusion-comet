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

Run the following command from the root of this repository to build the Comet Docker image, or use a [published
Docker image](https://hub.docker.com/r/apache/datafusion-comet).

```shell
docker build -t apache/datafusion-comet -f kube/Dockerfile .
```

## Example Spark Submit

The exact syntax will vary depending on the Kubernetes distribution, but an example `spark-submit` command can be
found [here](https://github.com/apache/datafusion-comet/tree/main/benchmarks).

## Helm chart

Install helm Spark operator for Kubernetes
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator

helm repo update

helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace --set webhook.enable=true
````

Check the operator is deployed
```bash
helm status --namespace spark-operator my-release

NAME: my-release
NAMESPACE: spark-operator
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

Create example Spark application file `spark-pi.yaml`
```bash
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: apache/datafusion-comet:0.7.0-spark3.5.4-scala2.12-java11
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.4.jar
  sparkConf:
    "spark.executor.extraClassPath": "/opt/spark/jars/comet-spark-spark5.4_2.12-0.7.0.jar"
    "spark.driver.extraClassPath": "/opt/spark/jars/comet-spark-spark3.5_2.12-0.7.0.jar"
    "spark.plugins": "org.apache.spark.CometPlugin"
    "spark.comet.enabled": "true"
    "spark.comet.exec.enabled": "true"
    "spark.comet.cast.allowIncompatible": "true"
    "spark.comet.exec.shuffle.enabled": "true"
    "spark.comet.exec.shuffle.mode": "auto"
    "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
  sparkVersion: 3.5.4
  driver:
    labels:
      version: 3.5.4
    cores: 1
    coreLimit: 1200m
    memory: 512m
    serviceAccount: spark-operator-spark
  executor:
    labels:
      version: 3.5.4
    instances: 1
    cores: 2
    coreLimit: 1200m
    memory: 512m
```
Refer to [Comet builds](#comet-docker-images)

Run Apache Spark application with Comet enabled
```bash
kubectl apply -f spark-pi.yaml
```

Check application status
```bash
kubectl describe sparkapplication --namespace=spark-operator
```
More info on [Kube Spark operator](https://www.kubeflow.org/docs/components/spark-operator/getting-started/)