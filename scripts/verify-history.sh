#!/usr/bin/env bash
# Enforce the icegres pre-merge git-history contract (see CLAUDE.md).
# Checks every commit on the current branch that is not already on the base ref.
# Usage: scripts/verify-history.sh [base-ref]   (default: origin/main)
set -uo pipefail

BASE="${1:-origin/main}"
RANGE="${BASE}..HEAD"

# Fall back to whole-branch history if the base ref is unknown or unrelated.
if ! git rev-parse --verify -q "$BASE" >/dev/null || \
   [ -z "$(git merge-base "$BASE" HEAD 2>/dev/null)" ]; then
  echo "note: '$BASE' not a shared ancestor — checking all reachable commits"
  RANGE="HEAD"
fi

SUBJECT_RE='^(feat|fix|docs|chore|test|refactor|perf|build|ci|style)(\([a-z0-9-]+\))?!?: .+'
BANNED='\bphase\b|\bP[1-9]\b|P5\+P7|\bRound [0-9]|\bR[0-9]{1,2}\b|claude-[a-z0-9-]*[0-9]|\bFable\b'
fail=0
n=0

for sha in $(git rev-list --no-merges "$RANGE"); do
  n=$((n+1))
  subject=$(git show -s --format='%s' "$sha")
  body=$(git show -s --format='%b' "$sha")
  short=${sha:0:9}

  # 1. Conventional subject
  echo "$subject" | grep -qE "$SUBJECT_RE" || { echo "✗ $short subject not conventional: $subject"; fail=1; }
  # 2. Subject length
  [ "${#subject}" -le 72 ] || { echo "✗ $short subject >72 chars (${#subject}): $subject"; fail=1; }
  # 3. Body present (message beyond subject + trailers)
  bodytext=$(printf '%s\n' "$body" | grep -vE '^(Co-Authored-By|Claude-Session):' | grep -vE '^[[:space:]]*$' || true)
  [ -n "$bodytext" ] || { echo "✗ $short has no commit body: $subject"; fail=1; }
  # 4. No phase / milestone / model tokens (subject + body)
  if printf '%s\n%s\n' "$subject" "$body" | grep -qiE "$BANNED"; then
    echo "✗ $short contains a banned token (phase/round/model): $subject"; fail=1
  fi
  # 5. Required trailers
  echo "$body" | grep -q '^Co-Authored-By:' || { echo "✗ $short missing Co-Authored-By trailer: $subject"; fail=1; }
  echo "$body" | grep -q '^Claude-Session:' || { echo "✗ $short missing Claude-Session trailer: $subject"; fail=1; }
done

# 6. Linear history (no merge commits in range)
if [ "$(git rev-list --merges "$RANGE" | wc -l)" -ne 0 ]; then
  echo "✗ history contains merge commits — keep the branch linear"; fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo "OK: $n commit(s) satisfy the pre-merge history contract."
else
  echo "FAIL: history contract violations above. See CLAUDE.md."
fi
exit "$fail"
