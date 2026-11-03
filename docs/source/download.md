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

# Download

Apache DataFusion Comet is released as source code. The current release is **1.2.0**.

## Source release

- [apache-datafusion-comet-1.2.0.tar.gz](https://www.apache.org/dyn/closer.lua/datafusion/datafusion-comet-1.2.0/apache-datafusion-comet-1.2.0.tar.gz?action=download) (source tarball)
- [apache-datafusion-comet-1.2.0.tar.gz.asc](https://downloads.apache.org/datafusion/datafusion-comet-1.2.0/apache-datafusion-comet-1.2.0.tar.gz.asc) (PGP signature)
- [apache-datafusion-comet-1.2.0.tar.gz.sha512](https://downloads.apache.org/datafusion/datafusion-comet-1.2.0/apache-datafusion-comet-1.2.0.tar.gz.sha512) (SHA-512 checksum)

The tarball is served from the nearest Apache mirror. The signature and checksum are served by the Apache Software
Foundation and must not be taken from a mirror. The public keys of the release managers are in the
[KEYS](https://downloads.apache.org/datafusion/KEYS) file.

The release notes are in the [changelog](changelog/1.2.0.md).
See [Building Comet From Source](https://datafusion.apache.org/comet/user-guide/1.1/source.html) for how to build the tarball.

## Verifying the download

It is essential that you verify the integrity of the downloaded file using the PGP signature and the checksum.

```console
# Import the release managers' public keys
curl -O "https://downloads.apache.org/datafusion/KEYS"
gpg --import KEYS
# Verify the signature and the checksum
gpg --verify apache-datafusion-comet-1.2.0.tar.gz.asc apache-datafusion-comet-1.2.0.tar.gz
shasum -a 512 --check apache-datafusion-comet-1.2.0.tar.gz.sha512
```

The `gpg --verify` command should report a good signature. See the
[ASF guide to verifying releases](https://www.apache.org/info/verification.html) for more detail.

## Convenience binaries

Pre-built Comet jar files for Linux are published to
[Maven Central](https://central.sonatype.com/namespace/org.apache.datafusion) as a convenience. They are built from
the source release above. See the [installation guide](https://datafusion.apache.org/comet/user-guide/1.1/installation.html) for the links and for the
supported Spark versions.

## Older releases

Only the current release is kept on the mirrors. All earlier releases are permanently available from the
[Apache archive](https://archive.apache.org/dist/datafusion/).
