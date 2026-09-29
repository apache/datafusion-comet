#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
from collections import Counter
from github import Github
import os
import re
import subprocess

def print_pulls(repo_name, title, pulls):
    if len(pulls)  > 0:
        print("**{}:**".format(title))
        print()
        for (pull, authors) in pulls:
            url = "https://github.com/{}/pull/{}".format(repo_name, pull.number)
            print("- {} [#{}]({}) ({})".format(pull.title, pull.number, url, ", ".join(authors)))
        print()


def pull_authors(repo, pull, commit, participants):
    """
    Return {login: name} for the GitHub users who authored commits in a PR, starting with the
    author of the commit that merged it.
    """
    authors = {commit.author.login: commit.commit.author.name}

    # a squash merge has a Co-authored-by trailer for each other author in the PR, so the PR's
    # commits only need fetching when there is one
    if not re.search(r"^co-authored-by:", commit.commit.message, re.IGNORECASE | re.MULTILINE):
        return authors

    for c in pull.get_commits():
        # skip merges of the base branch into the PR, and commits whose email is not linked to a
        # GitHub account
        if len(c.parents) == 1 and c.author is not None:
            authors.setdefault(c.author.login, c.commit.author.name)

    # GitHub links a commit to whichever account has its email address, so a placeholder identity
    # or an AI agent can map to an account that has nothing to do with the project. Only credit
    # accounts that have opened an issue or PR here.
    for login in list(authors)[1:]:
        if login not in participants:
            # not totalCount, which is 0 when GitHub pages the results with a cursor
            participants[login] = len(repo.get_issues(creator=login, state="all").get_page(0)) > 0
        if not participants[login]:
            print(f"Not crediting {login} ({authors.pop(login)}) on #{pull.number}: they have not opened "
                  f"an issue or PR in {repo.full_name}", file=sys.stderr)
    return authors


def generate_changelog(repo, repo_name, tag1, tag2, version):

    # get a list of commits between two tags
    print(f"Fetching list of commits between {tag1} and {tag2}", file=sys.stderr)
    comparison = repo.compare(tag1, tag2)

    # get the pull requests for these commits
    print("Fetching pull requests", file=sys.stderr)
    unique_pulls = []
    all_pulls = []
    # the name git shortlog credits each commit author under
    names = {}
    # whether each co-author has opened an issue or PR in the repository
    participants = {}
    for commit in comparison.commits:
        if commit.author is not None:
            names.setdefault(commit.author.login, commit.commit.author.name)
        pulls = commit.get_pulls()
        for pull in pulls:
            # there can be multiple commits per PR if squash merge is not being used and
            # in this case we should get all the author names, but for now just pick one
            if pull.number not in unique_pulls:
                unique_pulls.append(pull.number)
                all_pulls.append((pull, pull_authors(repo, pull, commit, participants)))

    # we split the pulls into categories
    breaking = []
    bugs = []
    docs = []
    enhancements = []
    performance = []
    other = []

    # categorize the pull requests based on GitHub labels
    print("Categorizing pull requests", file=sys.stderr)
    for (pull, authors) in all_pulls:

        # see if PR title uses Conventional Commits
        cc_type = ''
        cc_scope = ''
        cc_breaking = ''
        parts = re.findall(r'^([a-z]+)(\([a-z]+\))?(!)?:', pull.title)
        if len(parts) == 1:
            parts_tuple = parts[0]
            cc_type = parts_tuple[0] # fix, feat, docs, chore
            cc_scope = parts_tuple[1] # component within project
            cc_breaking = parts_tuple[2] == '!'

        labels = [label.name for label in pull.labels]
        if 'api change' in labels or cc_breaking:
            breaking.append((pull, authors))
        elif 'performance' in labels or cc_type == 'perf':
            performance.append((pull, authors))
        elif 'bug' in labels or cc_type == 'fix':
            bugs.append((pull, authors))
        elif 'enhancement' in labels or cc_type == 'feat':
            enhancements.append((pull, authors))
        elif 'documentation' in labels or cc_type == 'docs' or cc_type == 'doc':
            docs.append((pull, authors))
        else:
            other.append((pull, authors))

    # produce the changelog content
    print("Generating changelog content", file=sys.stderr)

    # ASF header
    print("""<!--
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
-->\n""")

    print(f"# DataFusion Comet {version} Changelog\n")

    # get the number of commits
    commit_count = subprocess.check_output(f"git log --pretty=oneline {tag1}..{tag2} | wc -l", shell=True, text=True).strip()

    # credit each person once for every commit they authored, and once for every PR by someone
    # else that has commits of theirs
    credits = Counter()
    for line in subprocess.check_output(f"git shortlog -sn {tag1}..{tag2}", shell=True, text=True).splitlines():
        count, name = line.split("\t", 1)
        credits[name] += int(count)
    for (pull, authors) in all_pulls:
        # git shortlog has already counted the merge commit's author, who comes first. Count each
        # name once, as git shortlog does, so that someone with two accounts is not counted twice.
        (merge_author, *others) = [names.get(login, name) for (login, name) in authors.items()]
        for name in set(others) - {merge_author}:
            credits[name] += 1

    print(f"This release consists of {commit_count} commits from {len(credits)} contributors. "
          f"See credits at the end of this changelog for more information.\n")

    print_pulls(repo_name, "Breaking changes", breaking)
    print_pulls(repo_name, "Fixed bugs", bugs)
    print_pulls(repo_name, "Performance related", performance)
    print_pulls(repo_name, "Implemented enhancements", enhancements)
    print_pulls(repo_name, "Documentation updates", docs)
    print_pulls(repo_name, "Other", other)

    # show code contributions
    print("## Credits\n")
    print("Thank you to everyone who contributed to this release. Here is a breakdown of commits (PRs merged) "
          "per contributor. A PR with commits from more than one person counts for each of them.\n")
    print("```")
    for (name, count) in sorted(credits.items(), key=lambda item: (-item[1], item[0])):
        print(f"{count:6d}\t{name}")
    print("```\n")

    print("Thank you also to everyone who contributed in other ways such as filing issues, reviewing "
          "PRs, and providing feedback on this release.\n")

def resolve_ref(ref):
    """Resolve a git ref (e.g. HEAD, branch name) to a full commit SHA."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", ref], text=True
        ).strip()
    except subprocess.CalledProcessError:
        # If it can't be resolved locally, return as-is (e.g. a tag name
        # that the GitHub API can resolve)
        return ref


def cli(args=None):
    """Process command line arguments."""
    if not args:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("tag1", help="The previous commit or tag (e.g. 0.1.0)")
    parser.add_argument("tag2", help="The current commit or tag (e.g. HEAD)")
    parser.add_argument("version", help="The version number to include in the changelog")
    args = parser.parse_args()

    # Resolve refs to SHAs so the GitHub API compares the same commits
    # as the local git log. Without this, refs like HEAD get resolved by
    # the GitHub API to the default branch instead of the current branch.
    tag1 = resolve_ref(args.tag1)
    tag2 = resolve_ref(args.tag2)

    token = os.getenv("GITHUB_TOKEN")
    project = "apache/datafusion-comet"

    g = Github(token)
    repo = g.get_repo(project)
    generate_changelog(repo, project, tag1, tag2, args.version)

if __name__ == "__main__":
    cli()