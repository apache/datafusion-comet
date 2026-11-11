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

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { typeLabelsToAdd } from './pr-type-label.mjs';

test('adds the type label for a conventional title with no labels', () => {
  assert.deepEqual(typeLabelsToAdd('fix: keep the entry alive', []), ['bug']);
  assert.deepEqual(typeLabelsToAdd('perf(shuffle)!: reuse contexts', ['area:shuffle']), [
    'enhancement',
    'performance',
  ]);
  assert.deepEqual(typeLabelsToAdd('chore(deps): bump actions', []), ['enhancement']);
});

test('leaves a type label a maintainer already applied, whatever the event carried', () => {
  // The event payload showed no labels, but the pull request has one by the time the run starts.
  assert.deepEqual(typeLabelsToAdd('fix: keep the entry alive', ['enhancement']), []);
  assert.deepEqual(typeLabelsToAdd('perf: reuse contexts', ['performance']), []);
});

test('adds nothing for a title without a conventional prefix', () => {
  assert.deepEqual(typeLabelsToAdd('Add Lance contrib build gate', []), []);
  assert.deepEqual(typeLabelsToAdd('[draft] ci: pinned image', []), []);
});

test('does not repeat labels that are already present', () => {
  assert.deepEqual(typeLabelsToAdd('test: cover struct columns', ['test']), []);
});
