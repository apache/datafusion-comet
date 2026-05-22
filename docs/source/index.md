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

<section class="comet-hero comet-hero--terminal">
<div class="comet-hero__inner">

# Apache DataFusion Comet

<p class="comet-hero__tagline">A high-performance accelerator for Apache Spark</p>
<p class="comet-hero__lede">
Runs your existing Spark queries on the Apache DataFusion native engine, no code changes required. Also accelerates Parquet scans for Apache Iceberg.
</p>

<div class="comet-terminal" aria-label="Sample terminal session showing how to enable Comet">
<div class="comet-terminal__bar">
<span class="comet-terminal__dots" aria-hidden="true"><i></i><i></i><i></i></span>
<span class="comet-terminal__title">spark-shell &mdash; comet enabled</span>
</div>
<pre class="comet-terminal__body"><span class="term-line term-comment"># Download the Comet plugin for your Spark / Scala version</span>
<span class="term-line"><span class="term-prompt">$</span> <span class="term-var">export</span> COMET_JAR=comet-spark-spark4.1_2.13-0.16.0.jar</span>
<span class="term-line term-spacer"></span>
<span class="term-line term-comment"># Launch Spark with Comet enabled — drop-in, no code changes</span>
<span class="term-line"><span class="term-prompt">$</span> $SPARK_HOME/bin/spark-shell \</span>
<span class="term-line term-indent">--jars $COMET_JAR \</span>
<span class="term-line term-indent">--conf spark.driver.extraClassPath=$COMET_JAR \</span>
<span class="term-line term-indent">--conf spark.executor.extraClassPath=$COMET_JAR \</span>
<span class="term-line term-indent">--conf spark.plugins=org.apache.spark.CometPlugin \</span>
<span class="term-line term-indent">--conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \</span>
<span class="term-line term-indent">--conf spark.memory.offHeap.enabled=true \</span>
<span class="term-line term-indent">--conf spark.memory.offHeap.size=4g</span>
<span class="term-line term-spacer"></span>
<span class="term-line term-comment">// Your existing Spark queries — now accelerated by Comet via Apache DataFusion</span>
<span class="term-line"><span class="term-prompt">scala&gt;</span> spark.sql(<span class="term-str">"SELECT category, COUNT(*) FROM events GROUP BY category"</span>).show()</span>
<span class="term-line term-cursor"><span class="term-prompt">scala&gt;</span> <span class="term-blink">▍</span></span></pre>
</div>

<div class="comet-hero__ctas">
<a class="comet-cta comet-cta--primary" href="user-guide/latest/installation.html">Install Comet <span aria-hidden="true">→</span></a>
<a class="comet-cta comet-cta--secondary" href="https://github.com/apache/datafusion-comet" target="_blank" rel="noopener">View on GitHub</a>
</div>
<p class="comet-hero__meta">Apache 2.0 &nbsp;·&nbsp; Apache Software Foundation project &nbsp;·&nbsp; Runs on commodity hardware</p>
</div>
</section>

<section class="comet-perf">
<div class="comet-perf__inner comet-perf__inner--stacked">
<div class="comet-perf__head">
<p class="comet-perf__eyebrow">Run Spark Queries at DataFusion Speeds</p>
<p class="comet-perf__caption">Comet delivers a performance speedup for many queries, enabling faster data processing and shorter time-to-insights.</p>
<p class="comet-perf__detail">The chart below shows Comet accelerating TPC-DS @ 1 TB. See the <a href="https://datafusion.apache.org/comet/contributor-guide/benchmarking.html">Comet Benchmarking Guide</a> for the full per-query breakdown and reproduction methodology.</p>
</div>
<figure class="comet-perf__figure">
<img src="_static/images/benchmark-results/0.16.0/tpcds_allqueries.png"
     width="1000" height="600"
     loading="lazy" decoding="async"
     alt="Total time to run all TPC-DS queries — Comet versus stock Apache Spark" />
<figcaption>Total time to run all queries (lower is better).</figcaption>
</figure>
</div>
</section>

<section class="comet-feature">
<div class="comet-feature__inner">
<div class="comet-feature__copy">
<p class="comet-feature__eyebrow">Spark Compatibility</p>
<h2 class="comet-feature__title">100% compatibility with supported Spark versions.</h2>
<p class="comet-feature__body">Comet aims for 100% compatibility with all supported versions of Apache Spark, allowing you to integrate Comet into your existing Spark deployments and workflows seamlessly. With no code changes required, you can immediately harness the benefits of Comet's acceleration capabilities without disrupting your Spark applications. The Comet extension automatically detects unsupported features and falls back to the Spark engine.</p>
<p class="comet-feature__links">
<a href="user-guide/latest/compatibility/spark-versions.html">Spark version compatibility &rarr;</a>
</p>
</div>
</div>
</section>

<section class="comet-feature comet-feature--alt">
<div class="comet-feature__inner">
<div class="comet-feature__copy">
<p class="comet-feature__eyebrow">Use Commodity Hardware</p>
<h2 class="comet-feature__title">No GPUs. No FPGAs. No vendor lock-in.</h2>
<p class="comet-feature__body">Comet leverages commodity hardware, eliminating the need for costly hardware upgrades or specialized hardware accelerators, such as GPUs or FPGA. By maximizing the utilization of commodity hardware, Comet ensures cost-effectiveness and scalability for your Spark deployments.</p>
</div>
</div>
</section>

<section class="comet-feature">
<div class="comet-feature__inner">
<div class="comet-feature__copy">
<p class="comet-feature__eyebrow">Architecture</p>
<h2 class="comet-feature__title">Tight integration with Apache DataFusion.</h2>
<p class="comet-feature__body">Comet tightly integrates with the core Apache DataFusion project, leveraging its powerful execution engine. The diagram below shows how the Comet plugin intercepts Spark physical plans, translates supported operators into a protocol-buffer representation, and hands them to the Apache DataFusion native engine for execution.</p>
</div>
<figure class="comet-feature__figure">
<img src="_static/images/comet-overview.png"
     width="2569" height="2006"
     loading="lazy" decoding="async"
     alt="Comet architecture overview diagram showing the bridge between Apache Spark and Apache DataFusion" />
<figcaption>Comet Overview</figcaption>
</figure>
<p class="comet-feature__links">
<a href="contributor-guide/plugin_overview.html">How Comet works &rarr;</a>
</p>
</div>
</section>

<section class="comet-community">
<div class="comet-community__inner comet-community__inner--two">

<div class="comet-community__col">
<p class="comet-community__eyebrow">Getting Started</p>
<p class="comet-community__body">To get started with Apache DataFusion Comet, follow the <a href="user-guide/latest/installation.html">installation instructions</a>. Join the <a href="https://datafusion.apache.org/contributor-guide/communication.html">DataFusion Slack and Discord channels</a> to connect with other users, ask questions, and share your experiences with Comet.</p>
</div>

<div class="comet-community__col">
<p class="comet-community__eyebrow">Contributing</p>
<p class="comet-community__body">We welcome contributions from the community to help improve and enhance Apache DataFusion Comet. Whether it's fixing bugs, adding new features, writing documentation, or optimizing performance, your contributions are invaluable in shaping the future of Comet. Check out our <a href="contributor-guide/index.html">contributor guide</a> to get started.</p>
</div>

</div>
</section>

```{toctree}
:maxdepth: 1
:hidden:

User Guide <user-guide/index>
Contributor Guide <contributor-guide/index>
Changelog <changelog/index>
Comparison with Gluten <about/gluten_comparison>
Versioning Policy <about/versioning_policy>
ASF Links <asf/index>
```
