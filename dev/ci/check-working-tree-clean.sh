#!/bin/bash
set -euo pipefail  # Exit on errors, undefined vars, pipe failures

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "Error: Not in a git repository"
  exit 1
fi

# Check if prettier is installed
if ! command -v prettier &> /dev/null; then
  echo "Error: prettier is not installed. Please install it with: npm install -g prettier"
  exit 1
fi

# Format all markdown files in docs directory
echo "Formatting markdown files in docs directory..."
prettier -w "docs/source/**/*.md"

# Fail if there are any local changes (staged, unstaged, or untracked)
if [ -n "$(git status --porcelain)" ]; then
  echo "Working tree is not clean:"
  git status --short
  git diff
  echo ""
  echo "Please commit, stash, or clean these changes before proceeding."
  exit 1
else
  echo "Working tree is clean"
fi

