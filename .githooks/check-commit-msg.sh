#!/usr/bin/env bash
# Enforces: type(scope): description  (see docs/WRITING_STANDARDS.md)
msg=$(cat "$1")
pattern='^(feat|fix|docs|refactor|test|chore|data|style)\([a-z0-9_/.-]+\): .+'

if echo "$msg" | grep -qE '^(Merge|Revert|fixup!|squash!)'; then
  exit 0
fi

if ! echo "$msg" | grep -qE "$pattern"; then
  echo "ERROR: commit message does not match writing standards."
  echo "  Required: type(scope): imperative description"
  echo "  Types:    feat fix docs refactor test chore data style"
  echo "  Example:  feat(ingest): add NOAA AIS month backfill"
  echo "  Got:      $msg"
  exit 1
fi
