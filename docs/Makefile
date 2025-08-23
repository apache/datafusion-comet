# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# Minimal makefile for Sphinx documentation
#

SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Extract version from Cargo.toml (default)
PROJECT_VERSION := $(shell grep '^version' ../native/Cargo.toml | head -1 | cut -d '"' -f2)

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

html:
	@echo "Building docs for version $(PROJECT_VERSION)"
	@PROJECT_VERSION=$(PROJECT_VERSION) $(SPHINXBUILD) -b html "$(SOURCEDIR)" "$(BUILDDIR)/html"

# Build docs and copy into versioned dir (like Spark does)
publish: html
	@mkdir -p site/docs/$(PROJECT_VERSION)
	@cp -r $(BUILDDIR)/html/* site/docs/$(PROJECT_VERSION)/
	@rm -rf site/docs/latest
	@ln -s $(PROJECT_VERSION) site/docs/latest
	@echo "Docs published under site/docs/$(PROJECT_VERSION) and site/docs/latest"