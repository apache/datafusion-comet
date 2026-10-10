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

# Apache DataFusion Comet Documentation

This folder contains the source content for the Apache DataFusion Comet documentation site. This content is published
to <https://datafusion.apache.org/comet> when any changes are merged into the main branch.

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

- Python
- `pip install -r requirements.txt`
- Node, and `npm install -g "$(python3 ../dev/ci/check-mermaid.py --cli-spec)"` for the `mmdc` command

`mmdc` draws the ` ```mermaid ` fences into SVG when the docs are built. Without it on
`PATH` the build still succeeds, but logs a warning and leaves those diagrams out of the pages.
That is silent all the way to the published site, so two CI checks guard it, both of which you
can run yourself:

```bash
python3 ../dev/ci/check-mermaid.py                      # every fence renders under mmdc
python3 ../dev/ci/check-mermaid.py --built build/html   # every fence reached a page
```

`mmdc` draws each diagram by driving headless Chrome. Installing mermaid-cli is supposed to fetch
that browser through puppeteer's `postinstall`, but that script catches its own download failures
and exits 0, so `Could not find chrome-headless-shell` from the check above means the install went
green without one. Fetch it explicitly with `npx puppeteer browsers install chrome-headless-shell`.

Chrome's sandbox also cannot start under the AppArmor policy Ubuntu ships from 23.10 onwards.
`puppeteer-config.json` turns it off, and `mermaid_params` in `source/conf.py` passes that config
to every render.

## Build & Preview

Run the provided script to build the HTML pages.

```bash
./build.sh
```

The HTML will be generated into a `build` directory.

Preview the site on Linux by running this command.

```bash
firefox build/html/index.html
```

## Making Changes

To make changes to the docs, simply make a Pull Request with your
proposed changes as normal. When the PR is merged the docs will be
automatically updated.

## Release Process

This documentation is hosted at <https://datafusion.apache.org/comet/>

When the PR is merged to the `main` branch of the `datafusion-comet`
repository, a [GitHub workflow](https://github.com/apache/datafusion-comet/blob/main/.github/workflows/docs.yaml) which:

1. Builds the html content
2. Pushes the html content to the [`asf-site`](https://github.com/apache/datafusion-comet/tree/asf-site) branch in this repository.

The Apache Software Foundation provides <https://datafusion.apache.org/>,
which serves content based on the configuration in
[.asf.yaml](https://github.com/apache/datafusion-comet/blob/main/.asf.yaml),
which specifies the target as <https://datafusion.apache.org/comet/>.
