#!/bin/bash
set -e
SOURCE_BRANCH="dev"
TARGET_BRANCH="main"


set +e
docker compose run --rm -T nessie-cli \
  --uri "http://nessie:19120/api/v2" \
  --non-ansi \
  -c "MERGE DRY BRANCH $SOURCE_BRANCH INTO $TARGET_BRANCH BEHAVIOR NORMAL"
DRY_STATUS=$?
set -e
if [ $DRY_STATUS -ne 0 ]; then
    printf "[ERROR] Thao tác DRY RUN thất bại. Phát hiện xung đột (Conflict) giữa nhánh '%s' và '%s'.\n" "$SOURCE_BRANCH" "$TARGET_BRANCH"
    printf "[INFO] Đã hủy tiến trình đồng bộ. Nhánh đích '%s' không bị thay đổi.\n" "$TARGET_BRANCH"
    exit 1
fi

docker compose run --rm -T -e TERM=dumb nessie-cli \
  --uri "http://nessie:19120/api/v2" \
  --non-ansi \
  -c "MERGE BRANCH $SOURCE_BRANCH INTO $TARGET_BRANCH BEHAVIOR NORMAL" \
  -c "DROP BRANCH $SOURCE_BRANCH" \
  -c "CREATE BRANCH $SOURCE_BRANCH FROM $TARGET_BRANCH"
printf "✅ [SUCCESS] Đã hoàn tất việc merge nhánh '%s' vào '%s' và xóa nhánh '%s'.\n" "$SOURCE_BRANCH" "$TARGET_BRANCH" "$SOURCE_BRANCH"

  