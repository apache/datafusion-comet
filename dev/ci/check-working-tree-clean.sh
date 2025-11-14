#!/bin/bash
set -euo pipefail  # Exit on errors, undefined vars, pipe failures

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "Error: Not in a git repository"
  exit 1
fi

# Fail if there are any local changes (staged, unstaged, or untracked)
if [ -n "$(git status --porcelain)" ]; then
  echo "Working tree is not clean:"
  git status --short
  echo ""
  echo "Please commit, stash, or clean these changes before proceeding."
  exit 1
else
  echo "Working tree is clean"
fi

