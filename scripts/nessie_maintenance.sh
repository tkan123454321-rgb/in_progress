# !/bin/bash
set -e
if [ -f .env ]; then
    echo "📖 [SYSTEM] Đang nạp cấu hình từ file .env..."
    # Dùng lệnh source để đưa các biến vào phiên làm việc hiện tại
    source .env
else
    echo "❌ [ERROR] Không tìm thấy file .env! Vui lòng kiểm tra lại."
    exit 1
fi

JDBC_ARGS=(
  "--jdbc-url" "jdbc:postgresql://postgres:5432/platform_db?currentSchema=nessie_gc"
  "--jdbc-user" "${POSTGRES_USER}"
  "--jdbc-password" "${POSTGRES_PASSWORD}"
)

if date --version >/dev/null 2>&1; then
    SAFE_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ" -d "7 days ago") # Cho Linux
else
    SAFE_TIME=$(date -u -v-7d +"%Y-%m-%dT%H:%M:%SZ") # Cho Mac
fi

set +e 
docker compose run --rm nessie-gc create-sql-schema "${JDBC_ARGS[@]}" --jdbc-schema "DROP_AND_CREATE" > /dev/null 2>&1
DROP_STATUS=$?
if [ $DROP_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái xoá và tạo các table trong nessie_gc: THÀNH CÔNG (Mã: %d)\n" "$DROP_STATUS"
else
    printf "❌ [ERROR] Trạng thái xoá và tạo các table trong nessie_gc: THẤT BẠI (Mã: %d)\n" "$DROP_STATUS"
fi

set -e


# --- BƯỚC 2: QUÉT VÀ XÓA RÁC VẬT LÝ TRÊN MINIO ---
echo " Bước 2: Bắt đầu quét và xóa Orphan Files trên MinIO..."
docker compose run --rm nessie-gc gc \
  -c 'P7D' \
  --max-file-modification="$SAFE_TIME" \
  --uri "http://nessie:19120/api/v2" \
  "${JDBC_ARGS[@]}" \
  -I "s3.access-key-id=${MINIO_ROOT_USER}" \
  -I "s3.secret-access-key=${MINIO_ROOT_PASSWORD}" \
  -I "s3.endpoint=http://minio:9000" \
  -I "s3.path-style-access=true"

GC_STATUS=$?
if [ $GC_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái quét và xóa Orphan Files trên MinIO: THÀNH CÔNG (Mã: %d)\n" "$GC_STATUS"
else
    printf "❌ [ERROR] Trạng thái quét và xóa Orphan Files trên MinIO: THẤT BẠI (Mã: %d)\n" "$GC_STATUS"
fi
