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

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

function workflowText(name) {
  return readFileSync(path.join(__dirname, '../../.github/workflows', name), 'utf8');
}
// The bridge is one github-script step, so its `script: |` block is the code
// under test. Read it textually rather than parse the YAML: that keeps this test
// free of a YAML dependency.
function scriptOf(text) {
  const lines = text.split('\n');
  const start = lines.findIndex((line) => /^\s+script: \|\s*$/.test(line));
  assert.notEqual(start, -1, 'no `script: |` block');
  const indent = lines[start + 1].match(/^ */)[0].length;
  const body = [];
  for (const line of lines.slice(start + 1)) {
    if (line.trim() !== '' && !line.startsWith(' '.repeat(indent))) break;
    body.push(line.slice(indent));
  }
  return body.join('\n');
}
const bridgeScript = scriptOf(workflowText('fork_ci_status.yml'));
const ciConcurrency = /^concurrency:\n  group: (.*)\n  cancel-in-progress: (.*)$/m.exec(
  workflowText('ci.yml'));
assert.ok(ciConcurrency, 'ci.yml has no top-level concurrency block');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction('github', 'context', 'core', 'Date', bridgeScript);
const now = Date.UTC(2026, 9, 20, 12);
class Clock extends Date { static now() { return now; } }
const sha = 'a'.repeat(40);
const pr = {
  number: 10, state: 'open', updated_at: new Date(now).toISOString(),
  head: {
    sha, ref: 'feature',
    repo: {
      owner: { login: 'contributor' }, name: 'datafusion-comet',
      full_name: 'contributor/datafusion-comet',
    },
  },
  base: { ref: 'main' },
};
const run = (id = 1, extra = {}) => ({
  id, event: 'push', head_sha: sha, head_branch: 'feature', run_attempt: 1,
  status: 'completed', conclusion: 'success',
  html_url: 'https://github.com/contributor/datafusion-comet/actions/runs/' + id,
  ...extra,
});

async function execute({
  event = 'schedule', checks = [], branch = 'feature', runs = [run(1, { head_branch: branch })],
  workflowState = 'active',
  recent = [pr], slice = [pr], candidates = [pr], repo = 'datafusion-comet',
  owner = 'apache', runError, count = 1, notification = {}, checkErrors = {},
} = {}) {
  const writes = [], calls = [], failures = [];
  let sweepPage;
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  github.rest.checks.listForRef = async () => {};
  github.rest.pulls.list = async (params) => {
    calls.push(['pulls', params]);
    if (params.per_page === 1) return {
      data: count ? [pr] : [],
      headers: count ? { link: '<https://api.github.com/pulls?per_page=1&page=' + count + '>; rel="last"' } : {},
    };
    assert.equal(params.per_page, 25);
    sweepPage = params.page;
    return { data: slice };
  };
  github.rest.actions.listWorkflowRuns = async (params) => {
    calls.push(['runs', params]);
    assert.equal(params.owner, 'contributor');
    assert.equal(params.workflow_id, 'ci_fork.yml');
    assert.equal(params.head_sha, sha);
    assert.equal(params.branch, branch);
    if (runError) throw Object.assign(new Error('API failure'), { status: runError });
    return { data: { workflow_runs: runs } };
  };
  github.rest.actions.getWorkflow = async () => {
    if (workflowState === null) throw Object.assign(new Error('Not Found'), { status: 404 });
    return { data: { state: workflowState } };
  };
  github.rest.checks.create = async (payload) => writes.push({ method: 'create', ...payload });
  github.rest.checks.update = async (payload) => writes.push({ method: 'update', ...payload });
  github.paginate = async (fn, params) => {
    if (fn === github.rest.checks.listForRef) {
      calls.push(['checks', params]);
      if (checkErrors[params.ref]) throw new Error(checkErrors[params.ref]);
      return checks;
    }
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.head, 'contributor:' + branch);
    return candidates;
  };
  github.paginate.iterator = async function* () {
    for (let i = 0; i < recent.length; i += 100) yield { data: recent.slice(i, i + 100) };
  };
  await bridge(github, {
    repo: { owner, repo }, eventName: event,
    payload: { workflow_run: {
      event: 'pull_request', head_sha: sha, head_branch: branch,
      head_repository: pr.head.repo, pull_requests: [], ...notification,
    } },
  }, { info: () => {}, setFailed: (message) => failures.push(message) }, Clock);
  return { writes, calls, sweepPage, failures };
}
async function existing(runValue = run()) {
  const { writes } = await execute({ runs: [runValue] });
  return {
    id: 100, app: { slug: 'github-actions' }, external_id: 'comet-fork-ci:10',
    ...writes[0], conclusion: writes[0].conclusion || null,
  };
}

test('CI completion discovers a fork PR even when the event PR list is empty', async () => {
  const { writes } = await execute({ event: 'workflow_run' });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].head_sha, sha);
  assert.equal(writes[0].name, 'CI (fork)');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, run().html_url);
});
test('notifications ignore stale branch heads, release PRs, closed PRs and deleted forks', async () => {
  for (const candidate of [
    { ...pr, head: { ...pr.head, sha: 'b'.repeat(40) } },
    { ...pr, base: { ref: 'maintenance' } },
    { ...pr, state: 'closed' },
    { ...pr, head: { ...pr.head, repo: null } },
  ]) {
    assert.deepEqual((await execute({ event: 'workflow_run', candidates: [candidate] })).writes, []);
  }
});
test('upstream pushes and merge-queue completions cannot create fork checks', async () => {
  for (const event of ['push', 'merge_group']) {
    assert.deepEqual((await execute({ event: 'workflow_run', notification: { event } })).writes, []);
  }
});
test('only a push run for the exact branch head is mirrored', async () => {
  const result = await execute({ runs: [
    run(8, { head_sha: 'b'.repeat(40) }),
    run(7, { head_branch: 'other' }),
    run(6, { event: 'pull_request' }),
    run(5, { event: 'workflow_dispatch' }),
    run(1),
  ] });
  assert.equal(result.writes[0].details_url, run(1).html_url);
});
for (const [state, expected] of [
  ['active', 'neutral'], ['disabled_fork', 'action_required'], [null, 'action_required'],
]) {
  test('missing run reports workflow state ' + state, async () => {
    const { writes } = await execute({ runs: [], workflowState: state });
    assert.equal(writes[0].conclusion, expected);
    assert.match(writes[0].output.summary, /Enable workflows/);
    assert.equal(JSON.parse(writes[0].output.text).run_id, null);
  });
}
test('a missing workflow endpoint is handled as missing CI', async () => {
  const { writes } = await execute({ runError: 404, workflowState: null });
  assert.equal(writes[0].conclusion, 'action_required');
});
test('permission, rate limit and server errors are not reported as missing CI', async () => {
  for (const runError of [403, 429, 500]) {
    const result = await execute({ runError });
    assert.deepEqual(result.writes, []);
    assert.deepEqual(result.failures, ['#10: API failure']);
    await assert.rejects(execute({ event: 'workflow_run', runError }), /API failure/);
  }
});
for (const conclusion of ['failure', 'cancelled', 'timed_out', 'startup_failure', 'stale']) {
  test('mirrors ' + conclusion + ' without reporting success', async () => {
    const { writes } = await execute({ runs: [run(1, { conclusion })] });
    assert.equal(writes[0].conclusion,
      ['startup_failure', 'stale'].includes(conclusion) ? 'failure' : conclusion);
  });
}
test('a stale run updates an existing check with failure, not the reserved conclusion', async () => {
  const { writes } = await execute({
    checks: [await existing()], runs: [run(1, { conclusion: 'stale' })],
  });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'failure');
});
for (const source of ['recent', 'slice']) {
  test('the sweep continues after errors in the ' + source + ' PRs and reports every failure', async () => {
    const broken = ['b', 'c'].map((letter, index) => ({
      ...pr, number: 20 + index, head: { ...pr.head, sha: letter.repeat(40) },
    }));
    const result = await execute({
      recent: [], slice: [], [source]: [...broken, pr], count: 3,
      checkErrors: {
        [broken[0].head.sha]: 'First API failure',
        [broken[1].head.sha]: 'Second API failure',
      },
    });
    assert.equal(result.writes.length, 1);
    assert.equal(result.writes[0].external_id, 'comet-fork-ci:10');
    assert.equal(result.writes[0].conclusion, 'success');
    assert.deepEqual(result.failures, ['#20: First API failure\n#21: Second API failure']);
  });
}
test('a rerun of the same workflow run updates the existing check', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].check_run_id, 100);
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(JSON.parse(writes[0].output.text).run_attempt, 2);
});
test('a deleted run clears its stale success result', async () => {
  const { writes } = await execute({ checks: [await existing()], runs: [] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'neutral');
});
test('unchanged results do not consume check write requests', async () => {
  const check = await existing();
  // GitHub can replace details_url with the check's own URL.
  for (const details_url of [check.details_url, 'https://github.com/apache/datafusion-comet/runs/100']) {
    assert.deepEqual((await execute({ checks: [{ ...check, details_url }] })).writes, []);
  }
});
test('a same-name check from another app or PR is not overwritten', async () => {
  const check = await existing();
  for (const foreign of [
    { ...check, app: { slug: 'other-app' } },
    { ...check, external_id: 'comet-fork-ci:20' },
  ]) {
    assert.equal((await execute({ checks: [foreign] })).writes[0].method, 'create');
  }
});
test('recent completed checks wait for the rotating sweep', async () => {
  const { writes, calls } = await execute({ checks: [await existing()], slice: [] });
  assert.deepEqual(writes, []);
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 0);
});
test('late runs are recovered outside the sweep slice', async () => {
  const missing = await execute({ runs: [] });
  const check = { ...await existing(), ...missing.writes[0] };
  const { writes } = await execute({ checks: [check], slice: [] });
  assert.equal(writes[0].conclusion, 'success');
});
for (const status of ['queued', 'in_progress']) {
  test('polls recent ' + status + ' runs outside the sweep slice', async () => {
    const check = await existing(run(1, { status, conclusion: null }));
    const { writes } = await execute({ checks: [check], slice: [] });
    assert.equal(writes[0].conclusion, 'success');
  });
}
test('the sweep reaches old PRs beyond 500 open PRs', async () => {
  const old = { ...pr, updated_at: new Date(now - 24 * 3600 * 1000).toISOString() };
  const { writes, sweepPage } = await execute({ count: 600, recent: [old], slice: [old] });
  assert.equal(sweepPage, Math.floor(now / (15 * 60 * 1000)) % 24 + 1);
  assert.equal(writes.length, 1);
});
test('an active PR appearing in both lists is polled once', async () => {
  const { calls } = await execute();
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 1);
});
test('no open PRs means no fork API calls', async () => {
  const result = await execute({ count: 0 });
  assert.equal(result.calls.length, 1);
  assert.deepEqual(result.writes, []);
});

test('a PR from a branch called main, the default branch included, is mirrored', async () => {
  // ci_fork.yml runs every branch of a fork, so there is always one workflow to look up.
  const mainPr = { ...pr, head: { ...pr.head, ref: 'main' } };
  const { writes } = await execute({
    branch: 'main', recent: [mainPr], slice: [mainPr], candidates: [mainPr],
  });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, run().html_url);
});

function group(github) {
  return ciConcurrency[1].replace(/\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) => new Function('github', 'return (' + expression + ');')(github));
}
const forkPush = {
  // GitHub expressions resolve absent properties to an empty string.
  repository: 'contributor/datafusion-comet', event_name: 'push', event: { pull_request: { number: '' } },
  ref: 'refs/heads/feature', sha: 'first', run_id: 1, workflow: 'Comet CI (fork run)',
};
const pullRequest = (number, extra = {}) => ({
  ...forkPush, repository: 'apache/datafusion-comet', event_name: 'pull_request',
  event: { pull_request: { number } }, ref: 'refs/pull/' + number + '/merge', ...extra,
});
test('every fork push has a group of its own, so none cancels another', () => {
  // A backup branch created at the commit a pull request is building has the same
  // sha, so a group keyed on the commit would let its run cancel the build.
  // ci_fork.yml's supersede job cancels older runs of exactly one branch instead,
  // which a group cannot: names compare without regard to case.
  assert.notEqual(group(forkPush), group({ ...forkPush, run_id: 2 }));
  assert.notEqual(group(forkPush), group({ ...forkPush, run_id: 2, ref: 'refs/heads/backup' }));
  assert.notEqual(group(forkPush), group({ ...forkPush, sha: 'second', run_id: 2 }));
});
test('upstream pushes and queue runs stay separate per commit', () => {
  for (const event_name of ['push', 'merge_group']) {
    const upstream = { ...forkPush, repository: 'apache/datafusion-comet', event_name };
    assert.notEqual(group(upstream), group({ ...upstream, sha: 'second' }));
  }
  const upstream = { ...forkPush, repository: 'apache/datafusion-comet' };
  assert.notEqual(group(upstream), group({ ...upstream, event_name: 'merge_group' }));
});
test('a push to a pull request supersedes the last one, whatever the commit', () => {
  assert.equal(group(pullRequest(7, { sha: 'first' })), group(pullRequest(7, { sha: 'second' })));
});
test('same-named branches from different forks cannot cancel each other', () => {
  // github.head_ref is the branch name alone, so it is the same for both.
  const one = pullRequest(7, { head_ref: 'patch-1' });
  const other = pullRequest(8, { head_ref: 'patch-1' });
  assert.notEqual(group(one), group(other));
});
test('a label run is not cancelled by the commit run, or by another label', () => {
  const labeled = (name) => pullRequest(7, { event: { action: 'labeled', label: { name }, pull_request: { number: 7 } } });
  assert.notEqual(group(labeled('run-iceberg-tests')), group(pullRequest(7)));
  assert.notEqual(group(labeled('run-iceberg-tests')), group(labeled('run-delta-tests')));
});

// The supersede job in ci_fork.yml cancels older runs of exactly one branch.
const supersedeScript = scriptOf(workflowText('ci_fork.yml'));
const supersede = new AsyncFunction('github', 'context', 'core', supersedeScript);
async function runSupersede({ runs, cancelError, branch = 'feature', runNumber = 5 }) {
  const cancelled = [];
  const github = { rest: { actions: {} } };
  github.rest.actions.listWorkflowRuns = async () => {};
  github.rest.actions.cancelWorkflowRun = async ({ run_id }) => {
    if (cancelError) throw Object.assign(new Error('conflict'), { status: cancelError });
    cancelled.push(run_id);
  };
  let calls = 0;
  github.paginate = async (fn, params) => {
    assert.equal(fn, github.rest.actions.listWorkflowRuns);
    assert.equal(params.workflow_id, 'ci_fork.yml');
    // One listing per status, so each run appears once.
    return runs.filter((r) => r.status === params.status);
  };
  await supersede(github, { repo: { owner: 'contributor', repo: 'datafusion-comet' },
    ref: 'refs/heads/' + branch, runNumber }, { info: () => { calls += 1; } });
  return cancelled;
}
const branchRun = (id, extra = {}) => ({
  id, run_number: id, head_branch: 'feature', status: 'in_progress', ...extra,
});
test('a push cancels the older queued and running runs of its branch', async () => {
  const cancelled = await runSupersede({ runs: [
    branchRun(2), branchRun(3, { status: 'queued' }), branchRun(4),
  ] });
  assert.deepEqual(cancelled.sort(), [2, 3, 4]);
});
test('a push does not cancel itself, a newer run or a rerun of itself', async () => {
  const cancelled = await runSupersede({ runs: [branchRun(5), branchRun(6), branchRun(7)] });
  assert.deepEqual(cancelled, []);
});
test('a push cancels no run of a branch that differs only by case', async () => {
  const cancelled = await runSupersede({ runs: [
    branchRun(2, { head_branch: 'Feature' }), branchRun(3, { head_branch: 'other' }), branchRun(4),
  ] });
  assert.deepEqual(cancelled, [4]);
});
test('a run that finished before it was cancelled is not an error', async () => {
  const cancelled = await runSupersede({ runs: [branchRun(2)], cancelError: 409 });
  assert.deepEqual(cancelled, []);
  await assert.rejects(runSupersede({ runs: [branchRun(2)], cancelError: 500 }), /conflict/);
});
