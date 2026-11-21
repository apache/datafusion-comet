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

const HOUR = 3600 * 1000;
const FULL_CRON = '48 2-22/4 * * *';
// GraphQL nodes for the scheduled pass: a fork's check suite, the upstream
// `CI (fork)` check, and an open pull request holding both.
const suite = (id = 1, { status = 'COMPLETED', conclusion = status === 'COMPLETED' ? 'SUCCESS' : null,
  event = 'push', branch = 'feature', path = '.github/workflows/ci_fork.yml' } = {}) => ({
  status, conclusion, branch: { name: branch },
  workflowRun: { databaseId: id, event, url: run(id).html_url, file: { path } },
});
const forkCheck = ({
  id = 100, number = 10, status = 'COMPLETED', conclusion = status === 'COMPLETED' ? 'FAILURE' : null,
  runId = 1, text = JSON.stringify({ head_sha: sha, run_id: runId }),
} = {}) => ({ databaseId: id, status, conclusion, externalId: 'comet-fork-ci:' + number, text });
const page = (nodes, truncated = false) => ({ pageInfo: { hasNextPage: truncated }, nodes });
const prNode = ({
  number = 10, updated = now, suites = [suite()], checks = [], cross = true, base = 'main',
  headRef = 'feature', headOid = sha, suitesTruncated = false, checksTruncated = false,
} = {}) => ({
  number, updatedAt: new Date(updated).toISOString(), headRefOid: sha, isCrossRepository: cross,
  baseRefName: base, headRepository: { nameWithOwner: 'contributor/datafusion-comet' },
  statusCheckRollup: { commit: { oid: sha, checkSuites: page([{ checkRuns: page(checks, checksTruncated) }]) } },
  headRef: headRef && { name: headRef, target: { oid: headOid, checkSuites: page(suites, suitesTruncated) } },
});

async function execute({
  event = 'workflow_run', checks = [], branch = 'feature', runs = [run(1, { head_branch: branch })],
  workflowState = 'active', workflowError, schedule = '3,13,23,33,43,53 * * * *', nodes = [],
  candidates = [pr], repo = 'datafusion-comet', updated = (payload) => payload, writeErrors = {},
  owner = 'apache', runError, notification = {}, graphqlError,
} = {}) {
  const writes = [], calls = [], failures = [], warnings = [], infos = [];
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  github.rest.checks.listForRef = async () => {};
  github.rest.pulls.list = async () => {};
  github.rest.pulls.get = async (params) => {
    calls.push(['pull', params]);
    return { data: candidates.find((item) => item.number === params.pull_number) };
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
  github.rest.actions.getWorkflow = async (params) => {
    calls.push(['workflow', params]);
    assert.deepEqual(params, { owner: 'contributor', repo: 'datafusion-comet', workflow_id: 'ci_fork.yml' });
    if (workflowError) throw Object.assign(new Error('API failure'), { status: workflowError });
    if (workflowState === null) throw Object.assign(new Error('Not Found'), { status: 404 });
    return { data: { state: workflowState } };
  };
  github.rest.checks.create = async (payload) => writes.push({ method: 'create', ...payload });
  github.rest.checks.update = async (payload) => {
    if (writeErrors[payload.check_run_id]) throw new Error(writeErrors[payload.check_run_id]);
    writes.push({ method: 'update', ...payload });
    return { data: { conclusion: null, ...updated(payload) } };
  };
  github.paginate = async (fn, params) => {
    if (fn === github.rest.checks.listForRef) {
      calls.push(['checks', params]);
      return checks;
    }
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.head, 'contributor:' + branch);
    return candidates;
  };
  github.graphql = async (query, variables) => {
    calls.push(['graphql', variables]);
    assert.match(query, /orderBy: \{field: UPDATED_AT, direction: DESC\}/);
    assert.deepEqual([variables.owner, variables.repo], [owner, repo]);
    const start = Number(variables.cursor || 0);
    const end = start + variables.size;
    const response = { repository: { pullRequests: {
      pageInfo: { hasNextPage: end < nodes.length, endCursor: String(end) }, nodes: nodes.slice(start, end),
    } } };
    const failure = graphqlError?.(response, variables);
    if (failure) throw Object.assign(new Error('GraphQL failure'), failure);
    return response;
  };
  await bridge(github, {
    repo: { owner, repo }, eventName: event,
    payload: { schedule, workflow_run: {
      event: 'pull_request', head_sha: sha, head_branch: branch,
      head_repository: pr.head.repo, pull_requests: [], ...notification,
    } },
  }, { info: (message) => infos.push(message), warning: (message) => warnings.push(message),
    setFailed: (message) => failures.push(message) }, Clock);
  return { writes, calls, failures, warnings, infos };
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
test('the fork workflow is looked up only when no run is found', async () => {
  const lookups = ({ calls }) => calls.filter(([kind]) => kind === 'workflow').length;
  assert.equal(lookups(await execute()), 0);
  assert.equal(lookups(await execute({ runs: [] })), 1);
});
test('a missing workflow endpoint is handled as missing CI', async () => {
  const { writes } = await execute({ runError: 404, workflowState: null });
  assert.equal(writes[0].conclusion, 'action_required');
});
test('permission, rate limit and server errors are not reported as missing CI', async () => {
  for (const runError of [403, 429, 500]) {
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
test('a rerun of the same workflow run updates the existing check', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].check_run_id, 100);
  assert.equal(check.conclusion, 'failure');
  assert.equal(writes[0].conclusion, 'success');
  assert.deepEqual(JSON.parse(writes[0].output.text), { head_sha: sha, run_id: 1 });
});
test('a rerun attempt with the same result does not rewrite the check', async () => {
  const check = await existing(run(1));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.deepEqual(writes, []);
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
const callsOf = (calls, kind) => calls.filter(([name]) => name === kind);
// Only the GraphQL page is read: no fork run listing, workflow lookup or check listing.
const onlyGraphql = (calls) => calls.every(([kind]) => kind === 'graphql');
const sweep = ({ node, ...options } = {}) =>
  execute({ event: 'schedule', nodes: node && [prNode(node)], ...options });
const prs = (count, updated) =>
  Array.from({ length: count }, (_, index) => prNode({ number: 1000 + index, updated }));
test('a rerun result reaches a PR updated hours ago in one quick pass', async () => {
  const { writes, failures } = await sweep({ node: { updated: now - 7 * HOUR, checks: [forkCheck()] } });
  assert.deepEqual(writes.map((item) => [item.method, item.check_run_id, item.conclusion, item.details_url]),
    [['update', 100, 'success', run(1).html_url]]);
  assert.deepEqual(JSON.parse(writes[0].output.text), { head_sha: sha, run_id: 1 });
  assert.deepEqual(failures, []);
});
test('a completed check is left alone while its rerun is still going', async () => {
  for (const status of ['QUEUED', 'IN_PROGRESS']) {
    const { writes, calls } = await sweep({ node: { checks: [forkCheck()], suites: [suite(1, { status })] } });
    assert.deepEqual(writes, []);
    assert.ok(onlyGraphql(calls));
  }
});
test('a quick pass skips a PR updated days ago, and the full passes sync it', async () => {
  const node = { updated: now - 4 * 24 * HOUR, checks: [forkCheck()] };
  const quick = await sweep({ node });
  assert.deepEqual(quick.writes, []);
  assert.equal(callsOf(quick.calls, 'graphql').length, 1);
  for (const options of [{ schedule: FULL_CRON }, { event: 'workflow_dispatch' }]) {
    const { writes } = await sweep({ node, ...options });
    assert.deepEqual(writes.map((item) => item.conclusion), ['success']);
  }
});
test('unchanged checks cost one query and no other request', async () => {
  const { writes, calls, failures, infos } = await sweep({
    node: { checks: [forkCheck({ conclusion: 'SUCCESS' })] },
  });
  assert.deepEqual([writes, failures], [[], []]);
  assert.deepEqual(calls.map(([kind]) => kind), ['graphql']);
  assert.ok(infos.some((line) => /1 PRs checked, 0 skipped\b.*1 unchanged/.test(line)), infos.join('\n'));
});
test('a check stored by the previous format is not rewritten when its state holds', async () => {
  const text = JSON.stringify({ head_sha: sha, run_id: 1, run_attempt: 2 });
  const { writes } = await sweep({ node: { checks: [forkCheck({ conclusion: 'SUCCESS', text })] } });
  assert.deepEqual(writes, []);
});
test('a check of an older run in the same state moves to the newer run', async () => {
  const { writes } = await sweep({ node: { checks: [forkCheck({ conclusion: 'SUCCESS' })], suites: [suite(2)] } });
  assert.deepEqual(JSON.parse(writes[0].output.text), { head_sha: sha, run_id: 2 });
});
for (const [state, expected] of [
  ['active', 'neutral'], ['disabled_manually', 'action_required'], [null, 'action_required'],
]) {
  test('a scheduled pass reports a missing run with workflow state ' + state, async () => {
    const { writes, calls } = await sweep({
      workflowState: state, nodes: [prNode({ suites: [] }), prNode({ number: 11 })],
    });
    assert.deepEqual(calls.map(([kind]) => kind), ['graphql', 'workflow']);
    assert.deepEqual(writes.map((item) => [item.external_id, item.conclusion]), [
      ['comet-fork-ci:10', expected], ['comet-fork-ci:11', 'success'],
    ]);
    assert.equal(JSON.parse(writes[0].output.text).run_id, null);
  });
}
test('a run that appears after the no-run check replaces it', async () => {
  const { writes } = await sweep({ node: { checks: [forkCheck({ conclusion: 'NEUTRAL', runId: null })] } });
  assert.deepEqual(writes.map((item) => [item.method, item.conclusion]), [['update', 'success']]);
});
test('a scheduled pass reports workflow lookup errors instead of missing CI', async () => {
  for (const workflowError of [403, 429, 500]) {
    const { writes, failures } = await sweep({ workflowError, node: { suites: [] } });
    assert.deepEqual([writes, failures], [[], ['#10: API failure']]);
  }
});
test('a scheduled pass mirrors the newest push run of ci_fork.yml on the PR branch', async () => {
  const ignored = [suite(9, { branch: 'other' }), suite(8, { event: 'pull_request' }),
    suite(7, { path: '.github/workflows/ci.yml' })];
  const mirrored = await sweep({ node: { suites: [suite(2), ...ignored, suite(3), suite(1)] } });
  assert.equal(mirrored.writes[0].details_url, run(3).html_url);
  const none = await sweep({ node: { suites: ignored } });
  assert.equal(none.writes[0].conclusion, 'neutral');
});
test('a moved, gone, same-repository or release PR is skipped', async () => {
  const { writes, calls } = await sweep({ nodes: [
    prNode({ headOid: 'b'.repeat(40), checks: [forkCheck()] }), prNode({ number: 11, cross: false }),
    prNode({ number: 12, base: 'branch-1.0' }),
    prNode({ number: 13, headRef: null, checks: [forkCheck({ number: 13 })] }),
    { ...prNode({ number: 14, checks: [forkCheck({ number: 14 })] }), headRepository: null },
  ] });
  assert.deepEqual(writes, []);
  assert.ok(onlyGraphql(calls));
});
test('truncated fork suites, or a suite without its run or file, use the per-PR path', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  for (const node of [
    { suites: [suite(1)], suitesTruncated: true }, { suites: [{ ...suite(9), workflowRun: null }, suite(1)] },
    { suites: [{ ...suite(9), workflowRun: { ...suite(9).workflowRun, file: null } }, suite(1)] },
  ]) {
    const { calls, writes } = await sweep({ checks: [check], node: { ...node, checks: [forkCheck()] } });
    assert.deepEqual(callsOf(calls, 'pull').map(([, params]) => params.pull_number), [10]);
    assert.deepEqual(writes.map((item) => [item.check_run_id, item.conclusion]), [[100, 'success']]);
  }
});
test('a GraphQL error syncs the PRs that resolved and keeps paging, or ends the pass without a page', async () => {
  const graphqlError = ({ repository: { pullRequests } }, { cursor }) => !cursor && ({
    data: { repository: { pullRequests: { ...pullRequests, nodes: [null, ...pullRequests.nodes.slice(1)] } } },
    errors: [0, 1].map((index) => ({ path: ['repository', 'pullRequests', 'nodes', index, 'headRef'] })),
  });
  const { writes, failures, calls } = await sweep({ graphqlError, nodes: [prNode({ number: 20 }), ...prs(25)] });
  assert.deepEqual(callsOf(calls, 'graphql').map(([, params]) => params.cursor), [null, '25']);
  assert.deepEqual(writes.map((item) => item.external_id), prs(25).slice(1).map((item) => 'comet-fork-ci:' + item.number));
  assert.deepEqual(failures, ['#?: GraphQL failure\n#1000: GraphQL failure']);
  for (const path of [undefined, ['repository', 'pullRequests'], ['repository', 'pullRequests', 'nodes']]) {
    const graphqlError = (data) => ({ data: path && data, errors: path && [{ path }] });
    const result = await sweep({ graphqlError, node: {} });
    assert.deepEqual([result.writes, result.failures], [[], ['PR page: GraphQL failure']]);
  }
});
test('truncated upstream checks fail that PR and the pass still syncs the others', async () => {
  const { writes, failures } = await sweep({ nodes: [prNode({ number: 20, checksTruncated: true }), prNode()] });
  assert.deepEqual(writes.map((item) => item.external_id), ['comet-fork-ci:10']);
  assert.deepEqual(failures, ['#20: the head commit has more CI (fork) checks than a page holds']);
});
test('pages are read with cursors, and a quick pass stops at the first older PR', async () => {
  const all = await sweep({ nodes: prs(60) });
  assert.deepEqual(callsOf(all.calls, 'graphql').map(([, params]) => params.cursor ?? null), [null, '25', '50']);
  assert.ok(callsOf(all.calls, 'graphql').every(([, params]) => params.size === 25));
  const quick = await sweep({ nodes: [...prs(30), ...prs(30, now - 4 * 24 * HOUR)] });
  assert.equal(callsOf(quick.calls, 'graphql').length, 2);
  assert.ok(quick.infos.some((line) => /: 30 PRs checked/.test(line)), quick.infos.join('\n'));
});
test('a write error on one PR does not stop the pass', async () => {
  const { writes, failures } = await sweep({
    writeErrors: { 200: 'Write failure' },
    nodes: [prNode({ number: 20, checks: [forkCheck({ id: 200, number: 20 })] }), prNode({ checks: [forkCheck()] })],
  });
  assert.deepEqual(writes.map((item) => item.check_run_id), [100]);
  assert.deepEqual(failures, ['#20: Write failure']);
});
test('a check repeated in the response is written once', async () => {
  const node = prNode({ checks: [forkCheck()] });
  const { nodes } = node.statusCheckRollup.commit.checkSuites;
  nodes.push(nodes[0]);
  const { writes } = await sweep({ nodes: [node] });
  assert.equal(writes.length, 1);
});
test('an update GitHub does not apply is a warning, not a failure', async () => {
  const { writes, warnings, failures } = await sweep({
    updated: () => ({ status: 'completed', conclusion: 'failure' }), node: { checks: [forkCheck()] },
  });
  assert.deepEqual([writes.length, warnings.length, failures], [1, 1, []]);
});
test('a found run creates a check when the commit has only another PR\'s', async () => {
  const checks = [forkCheck({ number: 20, conclusion: 'SUCCESS' })];
  const { writes } = await sweep({ node: { checks, suites: [suite(1, { status: 'IN_PROGRESS' })] } });
  const fields = ({ method, name, head_sha, external_id, status }) => [method, name, head_sha, external_id, status];
  assert.deepEqual(writes.map(fields), [['create', 'CI (fork)', sha, 'comet-fork-ci:10', 'in_progress']]);
});

test('a PR from a branch called main, the default branch included, is mirrored', async () => {
  // ci_fork.yml runs every branch of a fork, so there is always one workflow to look up.
  const mainPr = { ...pr, head: { ...pr.head, ref: 'main' } };
  const { writes } = await execute({
    branch: 'main', candidates: [mainPr],
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
