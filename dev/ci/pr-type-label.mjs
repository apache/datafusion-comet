// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Type label for a pull request from its conventional commit prefix, mirroring how
// maintainers label by hand. Used by .github/workflows/label_prs.yml and tested by
// pr-type-label.test.mjs.

export const LABELS_BY_PREFIX = {
  feat: ['enhancement'],
  refactor: ['enhancement'],
  chore: ['enhancement'],
  fix: ['bug'],
  perf: ['enhancement', 'performance'],
  test: ['enhancement', 'test'],
  doc: ['documentation'],
  docs: ['documentation'],
  ci: ['enhancement', 'build'],
  build: ['enhancement', 'build'],
  deps: ['dependencies'],
};

const TYPE_LABELS = new Set(Object.values(LABELS_BY_PREFIX).flat());

const PREFIX = /^(feat|fix|perf|test|docs?|chore|refactor|ci|build|deps)(\([^)]*\))?!?:/i;

// Labels to add for `title` given the pull request's current labels. Empty when the title has
// no conventional prefix or when any type label is already present, so a choice a maintainer
// has made stays as it is.
export function typeLabelsToAdd(title, currentLabels) {
  const match = PREFIX.exec(title.trim());
  if (!match) {
    return [];
  }
  const present = new Set(currentLabels);
  if ([...present].some((label) => TYPE_LABELS.has(label))) {
    return [];
  }
  return LABELS_BY_PREFIX[match[1].toLowerCase()].filter((label) => !present.has(label));
}
