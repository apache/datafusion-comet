<!--
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

# Cloud Credential Providers

Comet's native S3 readers normally fetch credentials from the standard AWS credential chain
(static keys, instance profiles, environment variables, etc.). Some clusters use a vendor-managed
mechanism instead, where credentials are issued per request based on a JWT or per S3 path. For
those clusters, Comet supports loading a vendor-supplied bridge JAR that routes every native
credential request through the vendor's Java code.

## Do I need this?

You don't, if any of the following describe your cluster:

- You use static AWS credentials (`fs.s3a.access.key` / `fs.s3a.secret.key`).
- You use EC2 instance profiles, EKS pod identities, ECS task roles, or environment variables.
- Your S3 access works in Spark today via the default AWS credential chain.

You probably do, if any of these are true:

- You have a Hadoop S3A custom signer configured (`fs.s3a.custom.signers=...`).
- You have a Spark `CloudCredentialsProvider` configured
  (`spark.security.credentials.providerList=...`) that issues a JWT for your cluster's STS
  service.
- You have a custom Iceberg `client.factory` that injects a configured S3 client.
- Spark queries against your S3 paths work, but the same queries with Comet enabled fail.

## How to tell

If you suspect you need this, two symptoms confirm it:

- **Comet S3 reads fail with 403 AccessDenied** while the same query without Comet succeeds.
- **Comet falls back to Spark execution** silently for scans against the affected paths.

If both Spark and Comet succeed against your S3 paths, you do not need a bridge JAR.

## How to enable it

Add the vendor-supplied bridge JAR to your Spark executor classpath:

```sh
spark-submit \
  --jars vendor-comet-bridge.jar \
  ...
```

Or via `spark.jars` in your `spark-defaults.conf`:

```
spark.jars=/path/to/vendor-comet-bridge.jar
```

That is the entire enablement step. There are no Spark or Comet config keys to set. Comet
discovers the bridge through `META-INF/services` on the classpath at executor startup.

## Where to get the bridge JAR

From the same vendor that supplies your Hadoop S3A signer or Iceberg client factory. OSS Comet
deliberately ships no vendor-specific bridges; the SPI is a contract, not a built-in. If your
authorization vendor does not yet provide a Comet bridge, refer them to the contributor guide page
on cloud credential providers.

## Verification

When the bridge is on the executor classpath and being used you should see, in the executor
logs:

- At startup:
  `Registered CometCloudCredentialProvider: <fully.qualified.VendorClassName>`
- The first time Comet performs an S3 access:
  `Fetching credentials for bucket=<...> path=<...>` (at debug level)

If neither log line appears and you expected the bridge to be in use, the JAR is missing from the
executor classpath or its `META-INF/services/org.apache.comet.cloud.CometCloudCredentialProvider`
entry is missing.

When the bridge is _not_ registered (the default), you will see exactly one line at startup:

```
No CometCloudCredentialProvider registered; native S3 readers will use the default AWS
credential chain
```

This is the expected behavior for clusters that do not need the bridge.

## Troubleshooting

**`Multiple CometCloudCredentialProvider impls on classpath: [...]`** at executor startup. Two or
more bridge JARs were found. Remove all but one. Comet does not chain providers; it fails fast to
prevent silent ambiguity.

**`No CometCloudCredentialProvider registered`** combined with `403 AccessDenied`. The bridge JAR
is not on the executor classpath. Re-check `--jars` / `spark.jars`. On YARN or Kubernetes, confirm
the JAR was actually shipped to the executor pods and not only available on the driver.

**Slow first query, fast subsequent queries.** Expected. The vendor's STS-vending service is
likely cold-started; subsequent fetches reuse cached credentials inside the vendor's provider.

**Credentials silently going stale during long-running jobs.** The bridge defaults to a 5-minute
maximum cache window in the Iceberg / opendal path when the vendor does not specify a credential
expiry. If you suspect the vendor is not setting an expiry, ask them to populate
`expirationEpochMillis` on every returned credential. The 5-minute floor is a safety net, not a
configurable knob.
