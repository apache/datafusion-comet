#!/bin/bash
# Fail if there are any local changes (staged, unstaged, or untracked)
if [ -n "$(git status --porcelain)" ]; then
  echo "❌ Working tree is not clean:"
  git status --short
  exit 1
else
  echo "✅ Working tree is clean"
fi

