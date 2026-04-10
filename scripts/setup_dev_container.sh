#!/bin/bash

set -e 
find . -type f -name "*.sh" -exec chmod +x {} +
# --- 0. Cài đặt công cụ hệ thống ----
if ! command -v make &> /dev/null; then
    echo "[SYSTEM] Đang cài đặt make..."
    # Dùng apt-get update && apt-get install để đảm bảo thông nòng
    apt-get update && apt-get install -y make
    echo "[SYSTEM] Cài đặt make hoàn tất."
else
    echo "[SYSTEM] Lệnh 'make' đã có sẵn. Bỏ qua."
fi
# --- 1. Môi trường INGEST ---
if [ ! -d ".venv_ingest" ]; then
    echo "📦 [INGEST] Chưa thấy venv, đang tạo mới và cài thư viện..."
    python3 -m venv .venv_ingest
    
    # Kích hoạt tạm thời để cài
    source .venv_ingest/bin/activate
    pip install -r requirements_ingest.txt
    deactivate
    
    echo "✅ [INGEST] Cài đặt xong."
else
    echo "⏩ [INGEST] Môi trường đã tồn tại. Bỏ qua."
fi

# --- 2. Môi trường DBT ---
if [ ! -d ".venv_dbt" ]; then
    echo "🛠️ [DBT] Chưa thấy venv, đang tạo mới và cài thư viện..."
    python3 -m venv .venv_dbt
    source .venv_dbt/bin/activate
    pip install -r requirements_dbt.txt
    deactivate
    
    echo "✅ [DBT] Cài đặt xong."
else
    echo "⏩ [DBT] Môi trường đã tồn tại. Bỏ qua."
fi

if [ ! -d ".venv_api" ]; then
    echo "🌐 [API] Chưa thấy venv, đang tạo mới và cài thư viện..."
    python3 -m venv .venv_api
    
    # Kích hoạt tạm thời để cài
    source .venv_api/bin/activate
    pip install -r requirements_api.txt
    deactivate
    
    echo "✅ [API] Cài đặt xong."
else
    echo "⏩ [API] Môi trường đã tồn tại. Bỏ qua."
fi

source .venv_dbt/bin/activate


dbt clean || true  
dbt deps
deactivate

echo "🎉 [AUTO-SETUP] Hoàn tất! Ông chủ có thể làm việc."

grep -qq "alias ai=" ~/.bashrc || echo 'alias ai="source /app/.venv_ingest/bin/activate"' >> ~/.bashrc
grep -qq "alias ad=" ~/.bashrc || echo 'alias ad="source /app/.venv_dbt/bin/activate"' >> ~/.bashrc
grep -qq "alias aa=" ~/.bashrc || echo 'alias aa="source /app/.venv_api/bin/activate"' >> ~/.bashrc

source ~/.bashrc
