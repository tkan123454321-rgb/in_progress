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

docker compose run --rm nessie-gc create-sql-schema "${JDBC_ARGS[@]}" --jdbc-schema "DROP_AND_CREATE" > /dev/null 2>&1 && DROP_STATUS=0 || DROP_STATUS=$?
if [ $DROP_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái xoá và tạo các table trong nessie_gc: THÀNH CÔNG (Mã: %d)\n" "$DROP_STATUS"
else
    printf "❌ [ERROR] Trạng thái xoá và tạo các table trong nessie_gc: THẤT BẠI (Mã: %d)\n" "$DROP_STATUS"
fi



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
  -I "s3.path-style-access=true" && GC_STATUS=0 || GC_STATUS=$?

if [ $GC_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái quét và xóa Orphan Files trên MinIO: THÀNH CÔNG (Mã: %d)\n" "$GC_STATUS"
else
    printf "❌ [ERROR] Trạng thái quét và xóa Orphan Files trên MinIO: THẤT BẠI (Mã: %d)\n" "$GC_STATUS"
fi


echo "\n🧹 Bước 3: Dọn dẹp siêu dữ liệu (Metadata) của Nessie..."
docker compose run --rm nessie-admin cleanup-repository \
  --referenced-grace=P1D \
  --commit-rate=50 \
  --obj-rate=1000 \
  --scan-obj-rate=2000 \
  --purge-obj-rate=500 && NESSIE_CLEANUP_STATUS=0 || NESSIE_CLEANUP_STATUS=$?

if [ $NESSIE_CLEANUP_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái dọn rác metadata Nessie: THÀNH CÔNG (Mã: %d)\n" "$NESSIE_CLEANUP_STATUS"
else
    printf "❌ [ERROR] Trạng thái dọn rác metadata Nessie: THẤT BẠI (Mã: %d)\n" "$NESSIE_CLEANUP_STATUS"
fi

echo -e "\n📝 Bước 4: Dọn dẹp sổ ghi chép nhiệm vụ (Catalog Tasks) trên nhánh main..."
docker compose run --rm nessie-admin delete-catalog-tasks \
  --ref='main' \
  --batch=500 > /dev/null 2>&1 && TASK_CLEANUP_STATUS=0 || TASK_CLEANUP_STATUS=$?

if [ $TASK_CLEANUP_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Dọn rác nhiệm vụ Catalog (nhánh main): THÀNH CÔNG (Mã: %d)\n" "$TASK_CLEANUP_STATUS"
else
    printf "❌ [ERROR] Dọn rác nhiệm vụ Catalog (nhánh main): THẤT BẠI (Mã: %d)\n" "$TASK_CLEANUP_STATUS"
fi

# --- BƯỚC 5: BẢO TRÌ BẢNG POSTGRES ---
echo "\n🧽 Bước 5: Chạy VACUUM ANALYZE để giải phóng ổ cứng Postgres..."
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d platform_db -c "VACUUM ANALYZE;" && VACUUM_STATUS=0 || VACUUM_STATUS=$?

if [ $VACUUM_STATUS -eq 0 ]; then
    printf "✅ [SUCCESS] Trạng thái giải phóng ổ cứng Postgres: THÀNH CÔNG (Mã: %d)\n" "$VACUUM_STATUS"
else
    printf "❌ [ERROR] Trạng thái giải phóng ổ cứng Postgres: THẤT BẠI (Mã: %d)\n" "$VACUUM_STATUS"
fi