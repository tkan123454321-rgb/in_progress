#!/bin/bash
set -e
SOURCE_BRANCH="dev"
TARGET_BRANCH="main"

docker compose run --rm -T -e TERM=dumb nessie-cli \
  --uri "http://nessie:19120/iceberg" \
  --non-ansi \
  -c "MERGE BRANCH $SOURCE_BRANCH INTO $TARGET_BRANCH BEHAVIOR FORCE" \
  -c "DROP BRANCH $SOURCE_BRANCH" \
  -c "CREATE BRANCH $SOURCE_BRANCH FROM $TARGET_BRANCH"
printf " [SUCCESS] Đã hoàn tất việc merge nhánh '%s' vào '%s' và xóa nhánh '%s'.\n" "$SOURCE_BRANCH" "$TARGET_BRANCH" "$SOURCE_BRANCH"
