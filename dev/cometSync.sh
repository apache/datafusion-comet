#!/bin/bash

set -e

function cherry_pick_message {
    cat <<EOD

  Unable to automatically apply individual cherry-pick commit
  1. Complete cherry-pick manually:
        git cherry-pick $latest
  2. Address merge conflicts and complete the cherry-pick
        git cherry-pick --continue
  3. Move the tracking tag
        git tag -d $MERGE_TAG && git push $REMOTE_FROM --delete $MERGE_TAG
        git tag $MERGE_TAG $latest && git push $REMOTE_FROM tag $MERGE_TAG

EOD
}

# Each branch has its own merge tag.  This is used to determine what the commit of the last sync.
CURRENT_BRANCH=$(git branch --show current)
if [[ "${CURRENT_BRANCH##*-apple}" == ${CURRENT_BRANCH} ]]; then
  echo "This script should only be run on apple-specific branches"
  exit 1
fi

# Use the current branch name to determine the tag used for tracking the last merged commit
MERGE_TAG="MERGED_$(echo $CURRENT_BRANCH | tr 'a-z' 'A-Z' | tr '-' '_' | tr '/' '_')"

# Use the current branch to determine the source branch by dropping the postfix
MERGE_BRANCH=$(echo $CURRENT_BRANCH | sed 's/-apple$//')

REMOTE_FROM=origin
MERGE_FROM=$REMOTE_FROM/$MERGE_BRANCH

# Fetch the latest from all branches
git fetch --all

# Fetch the merge tag
git tag -d $MERGE_TAG
git fetch $REMOTE_FROM $MERGE_TAG:$MERGE_TAG

# MANUAL-ONLY: Make sure that the branch is up to date.  Normally this is handled by Rio
#git pull && git rebase ${REMOTE_FROM}/${CURRENT_BRANCH}

echo "Checking tag ${MERGE_TAG} against ${MERGE_FROM}..."
MERGE_COMMIT=$(git ls-remote ${REMOTE_FROM} ${MERGE_TAG} | awk '{print $1}')
echo "... using commit ${MERGE_COMMIT}"
if [[ "x${MERGE_COMMIT}" == "x" ]]; then
   echo Unable to find remote tag $MERGE_TAG
   exit 3
fi
commits=( $(git log ${MERGE_COMMIT}..${MERGE_FROM} --oneline | awk '{print $1}') )
if [ ${#commits[@]} -eq 0 ]; then
  range=""
else
  commit_from=${commits[${#commits[@]} - 1]}
  commit_to=${commits[0]}

  if [ ${commit_from} == ${commit_to} ]; then
    #range=${commit_from}
    range="${commit_from}^..${commit_to}"
  else
    range="${commit_from}^..${commit_to}"
  fi

  originalCommit_to="$commit_to"
fi

#DEBUG
#echo "originalCommit_to: $originalCommit_to"
#exit 2

if [ -z "$range" ]; then
  echo "Nothing has changed"
  exit 0
fi

set +e
set -x

git log $range --oneline

function sparkDiff {
  git diff --name-only $range | grep '/diffs/3.4.3' || true
}

function singleCommit {
  echo "Trying single commit..." >&2

  commit_to=$(git log --oneline $range | tail -1 | awk '{print $1}')
  echo ${commit_from}^..${commit_to}
}

spark_diff=$(sparkDiff)
if [ "$spark_diff" ]; then
  range=$(singleCommit)
  spark_diff=$(sparkDiff)
  if [ "$spark_diff" ]; then
    latest=$(git log --oneline $range | tail -1 | awk '{print $1}')

    echo "WARNING: Spark diff has changed"
    cherry_pick_message

    exit 1
  fi
fi

git cherry-pick $range
if [ $? -ne 0 ]; then
  echo "Unabled to automaticaly apply cherry-pick range $range"
  git cherry-pick --abort

  # Pick the latest commit to cherry-pick
  latest=$(git log --oneline $range | tail -1 | awk '{print $1}')
  git cherry-pick $latest
  if [ $? -ne 0 ]; then
    cat <<EOD

  Unable to automatically apply individual cherry-pick commit
  1. Complete cherry-pick manually:
        git cherry-pick $latest
  2. Address merge conflicts and complete the cherry-pick
        git cherry-pick --continue
  3. Move the tracking tag
        git tag -d $MERGE_TAG && git push $REMOTE_FROM --delete $MERGE_TAG
        git tag $MERGE_TAG $latest && git push $REMOTE_FROM tag $MERGE_TAG

EOD

    git cherry-pick --abort
    exit 2
  fi
  wasBatch=""
else
  # Pick the latest commit from the range
  latest=$(git log --oneline $range | head -1 | awk '{print $1}')
fi

export PROFILES="-Pspark-3.4-apple"
make core jvm

# Work around for constant tweak of the user-guide configs
git checkout HEAD -- docs/source/user-guide/configs.md

# Move the tag for the next sync
git tag -d $MERGE_TAG && git push $REMOTE_FROM --delete $MERGE_TAG
git tag $MERGE_TAG $latest && git push $REMOTE_FROM tag $MERGE_TAG
echo "Tagged $latest"

# Push the updates
git push && git push $REMOTE_FROM $CURRENT_BRANCH

set +x

echo "latest:            $latest"
echo "originalCommit_to: $originalCommit_to"
if [[ "$latest" != "$originalCommit_to" ]]; then
  cat <<EOD

Unable to complete the cherry-pick as a batch.  Re-run sync

EOD
fi
