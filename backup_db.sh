# --- CẤU HÌNH ---
# 1. Tên Database ông muốn backup
DB_NAME="finance_db"   
BUCKET="s3://kho-cho-kiki-mouse-2025/backup" 
DB_HOST="localhost"
DB_USER="tkan"
DATE=$(date +"%Y%m%d_%H%M") 
FILE_NAME="${DB_NAME}_${DATE}.dump"
FORMAT='c'


echo "--- Bat dau backup luc: $DATE ---"

PG_DUMP_BIN=$(command -v pg_dump)
AWS_BIN=$(command -v aws)

# 2. Kiểm tra: "Chỉ cần thiếu 1 trong 2 là nghỉ"
if [ -z "$PG_DUMP_BIN" ] || [ -z "$AWS_BIN" ]; then
    echo "LỖI: thiếu pg_dump hoặc aws cli"
    exit 1
else
  $PG_DUMP_BIN -h "$DB_HOST" -U "$DB_USER" -F "$FORMAT" -f "/tmp/$FILE_NAME" "$DB_NAME"
  if [ $? -eq 0 ]; then
    echo "Hut du lieu THANH CONG! File: /tmp/$FILE_NAME"
    echo "Dang upload len S3..."
    $AWS_BIN s3 cp /tmp/$FILE_NAME "$BUCKET/$FILE_NAME"

    # BƯỚC 3: Xóa file rác ở máy mình đi
    rm /tmp/$FILE_NAME
  else
    echo "LOI: Khong hut duoc du lieu. Kiem tra lai ten DB hoac mat khau."
  fi
fi