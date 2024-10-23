#!/usr/bin/env bash

set -eux -o pipefail

# Figure out where the project root is
PROJECT_ROOT=$(git -C "$(dirname $0)" rev-parse --show-toplevel)

function exit_with_usage {
  set +x
  echo ""
  echo "get-patch-list.sh - tool for getting a patch list between last two tags"
  echo ""
  echo "usage:"
  cl_options="[--project-root <path>]"
  echo "get-patch-list.sh $cl_options"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --project-root)
      PROJECT_ROOT=$(readlink -f "$2")
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

pushd "$PROJECT_ROOT"

# Check whether the directory is clean. E.g. pending merges, uncommited changes, etc...
# https://unix.stackexchange.com/a/394674
git update-index --really-refresh || { popd; set +x; echo ""; echo "FAILED! The directory is unclean, please check the git status."; exit 1; }
git diff-index --quiet HEAD || { popd; set +x; echo ""; echo "FAILED! The directory is unclean, please check the git status."; exit 1; }

# Check whether the current branch is tracking a remote branch
REMOTE_BRANCH=$(git rev-parse --abbrev-ref --symbolic-full-name @{u} || { popd; set +x; echo ""; echo "FAILED! The current branch is not tracking any remote branch."; exit 1; })

git fetch

TO_HASH=$(git rev-parse --short=7 $(git describe --abbrev=0 --tags $REMOTE_BRANCH))
FROM_HASH=$(git rev-parse --short=7 $(git describe --abbrev=0 --tags $TO_HASH^))

BASE_URL=$(git ls-remote --get-url | sed "s/:/\//" | sed "s/git@/https:\/\//" | sed "s/\.git$/\/compare/")

popd

set +x
echo ""
echo "SUCCESS! Patch list for $REMOTE_BRANCH"
echo "$BASE_URL/$FROM_HASH...$TO_HASH"
