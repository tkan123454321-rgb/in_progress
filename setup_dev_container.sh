#!/bin/bash

set -e 

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

echo "🎉 [AUTO-SETUP] Hoàn tất! Ông chủ có thể làm việc."

grep -qq "alias ai=" ~/.bashrc || echo 'alias ai="source /app/.venv_ingest/bin/activate"' >> ~/.bashrc
grep -qq "alias ad=" ~/.bashrc || echo 'alias ad="source /app/.venv_dbt/bin/activate"' >> ~/.bashrc

source ~/.bashrc
