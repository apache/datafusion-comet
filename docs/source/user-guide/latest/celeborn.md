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

# Using Comet with Apache Celeborn

Comet accelerates query processing. [Apache Celeborn](https://celeborn.apache.org/) provides
remote storage for shuffle data.

## Support Status

Comet's [native shuffle](tuning/shuffle.md#native-shuffle) through Celeborn is unavailable
with the currently released Celeborn 0.6.x and 0.7.x clients.
Setting `spark.comet.shuffle.mode=native` does not change that.

You can still use Comet to accelerate supported scans, filters, and other query operators.
Shuffle is handled by Celeborn's existing Spark integration.

## Setup

Start with a running Celeborn service and a compatible [Comet installation](installation.md).
Celeborn is an optional application dependency that Comet does not bundle. Supply both the
Comet JAR and a Celeborn Spark client matching the application's Spark and Scala versions on
the startup classpaths of the driver and all executors.

For Spark 3, the shaded client coordinates in Maven Central are
`org.apache.celeborn:celeborn-client-spark-3-shaded_<scala-binary-version>:<celeborn-version>`.
For example, `org.apache.celeborn:celeborn-client-spark-3-shaded_2.12:0.7.0` supplies
`celeborn-client-spark-3-shaded_2.12-0.7.0.jar` for Spark 3 / Scala 2.12.

The example below uses Spark 3.5, Scala 2.12, and Celeborn 0.7.0:

1. Download the matching Comet JAR using the [installation guide](installation.md) and the
   shaded Celeborn client from Maven Central.
2. Install both JARs in `$SPARK_HOME/jars` on the driver and every executor, or include them
   in the Spark image used by those processes. For JARs installed elsewhere, set
   `spark.driver.extraClassPath` and `spark.executor.extraClassPath` to their paths before
   startup. Those paths must exist on the corresponding machines.
3. Start Spark with the settings below, replacing the Celeborn master endpoints with your
   deployment's values.

Spark 3.5 loads the shuffle manager before the executor's user-JAR classloader is initialized.
Supplying only `--jars` or `--packages` does not ensure that the manager and Celeborn client
are available at that point; use the startup classpaths above.

This example enables Comet with Spark shuffle backed by Celeborn, as described in
[Support Status](#support-status).

```shell
$SPARK_HOME/bin/spark-shell \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.celeborn.master.endpoints=celeborn-master-1:9097,celeborn-master-2:9097 \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.shuffle.enabled=true \
    --conf spark.comet.shuffle.mode=auto \
    --conf spark.comet.explain.fallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

Celeborn's shaded client requires the Kryo serializer; see the
[Celeborn deployment guide](https://github.com/apache/celeborn/blob/v0.7.0/docs/deploy.md#deploy-spark-client).
For another Spark or Scala version, select matching Comet and Celeborn artifacts rather than
reusing the Spark 3 / Scala 2.12 example.

Set the shuffle manager and Celeborn service configuration before creating the Spark context.
Keep your deployment's existing Celeborn authentication, storage, and recovery settings.

## Verifying the Shuffle Path

1. Run a query containing a shuffle, then inspect its executed plan in the Spark SQL UI or
   with `df.explain("formatted")`. With AQE, inspect the final plan after an action completes.
   Supported operators can appear as Comet nodes, while the shuffle appears as a plain
   `Exchange`. This is the expected result with the currently released Celeborn 0.6.x and
   0.7.x clients.
2. Inspect shuffle read/write bytes, records, and time in the Spark UI. Spark's remote-read
   byte counters alone cannot confirm that Celeborn stored the data: they also count local
   shuffle files fetched from another executor. Celeborn's fallback policy may select local
   Spark shuffle for some exchanges; use the check below to identify those shuffles.

If an operator you expected Comet to accelerate remains on Spark, use
`spark.comet.explain.fallback.enabled=true` to see the reasons in the driver log. See
[Understanding Comet Plans](understanding-comet-plans.md) for details.

### Checking for Local Fallback

For the Spark 3.5 / Celeborn 0.7.0 setup above, enable INFO logging for
`org.apache.spark.scheduler.DAGScheduler` on the driver before running the query, then:

1. Open the query in the Spark UI's SQL tab, follow its associated jobs, and identify the
   shuffle-writing stage you want to check.
2. Find that stage in the driver log. For example,
   `Submitting ShuffleMapStage 5 (MapPartitionsRDD[17] ...)` identifies its input RDD as 17.
   Find the corresponding
   `Registering RDD 17 (...) as input to shuffle 3` message. In this example, stage 5 writes
   shuffle 3. Match the IDs within the same application; the messages need not be adjacent.
3. Check Celeborn's driver logs for
   `Fallback to vanilla Spark SortShuffleManager for shuffle: 3`.
   This confirms that shuffle 3 selected local Spark shuffle. If fallback occurs with dynamic
   allocation enabled and no external shuffle service, Celeborn instead logs an ERROR
   containing `fallback to vanilla Spark SortShuffleManager for shuffle: 3`.

The IDs above are examples. Use the shuffle ID you found in step 2 when checking Celeborn's
[fallback messages](https://github.com/apache/celeborn/blob/v0.7.0/client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/SparkShuffleManager.java#L222-L234).
The absence of a fallback message does not prove that data was stored in Celeborn.

## Troubleshooting

| Symptom                                                            | What to check                                                                                                                                                                                                            |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Celeborn classes cannot be loaded when the application starts      | Supply the matching shaded client on both driver and executor startup classpaths. Comet does not bundle it.                                                                                                              |
| The plan contains `Exchange` rather than `CometExchange`           | This is expected with the currently released Celeborn 0.6.x and 0.7.x clients. Shuffle uses Celeborn's existing Spark integration; other supported operators can still run in Comet.                                     |
| Setting `spark.comet.shuffle.mode=native` does not change the plan | Native shuffle is disabled with these clients. Changing this setting does not enable it.                                                                                                                                 |
| No Comet operators appear in the plan                              | Check that the Comet plugin and native library loaded, that Comet execution is enabled, and that the query uses supported operators. Inspect the driver fallback explanations.                                           |
| Shuffle data is stored locally instead of in Celeborn              | Check Celeborn's fallback policy, partition-count threshold, worker availability, and quota. An effective `spark.celeborn.client.spark.shuffle.fallback.policy=ALWAYS` selects local shuffle; `AUTO` can also select it. |
| Changing the shuffle manager in the SQL session has no effect      | Set the manager before creating the Spark context. Restart the application to change it.                                                                                                                                 |
