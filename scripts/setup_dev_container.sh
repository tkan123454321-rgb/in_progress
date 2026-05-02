#!/bin/bash
# Exit immediately if a command exits with a non-zero status
set -e 

# Ensure all shell scripts have execution permissions
find . -type f -name "*.sh" -exec chmod +x {} +

# --- 0. Pre-flight Checks ---
if [ ! -f "pyproject.toml" ]; then
    echo "[ERROR] pyproject.toml not found. Please run this script from the project root."
    exit 1
fi

# Install 'make' if missing (essential for DE workflows)
if ! command -v make &> /dev/null; then
    echo "[SYSTEM] 'make' not found. Installing..."
    sudo apt-get update && sudo apt-get install -y make
fi


# --- 1. Ingest Environment Setup ---
INGEST_VENV=".venv_ingest"
if [ ! -d "$INGEST_VENV" ]; then
    echo "[INFO] Creating Virtual Environment for Ingest..."
    python3 -m venv $INGEST_VENV
    echo "[INFO] Installing Ingest & Dev dependencies..."
    $INGEST_VENV/bin/pip install -e ".[ingest,dev]"
    echo "[SUCCESS] Ingest environment ready."
else
    echo "[SKIP] Ingest environment already exists."
fi

# --- 2. dbt Environment Setup ---
DBT_VENV=".venv_dbt"
if [ ! -d "$DBT_VENV" ]; then
    echo "[INFO] Creating Virtual Environment for dbt..."
    python3 -m venv $DBT_VENV
    echo "[INFO] Installing dbt dependencies..."
    $DBT_VENV/bin/pip install -e ".[dbt]"
    echo "[SUCCESS] dbt environment ready."
else
    echo "[SKIP] dbt environment already exists."
fi

# --- 3. Initialize dbt Packages ---
echo "[INFO] Updating dbt dependencies (dbt deps)..."
source $DBT_VENV/bin/activate
dbt deps
dbt clean || true
deactivate

# --- 4. User Utilities & Shortcuts ---
echo "[INFO] Configuring shell aliases..."

# Use absolute paths ($PWD) to ensure aliases work from anywhere
grep -qq "alias ai=" ~/.bashrc || echo "alias ai='source /app/$INGEST_VENV/bin/activate'" >> ~/.bashrc
grep -qq "alias ad=" ~/.bashrc || echo "alias ad='source /app/$DBT_VENV/bin/activate'" >> ~/.bashrc

echo "------------------------------------------------------------"
echo "🎉 Setup Complete! Professional environment initialized."
echo "💡 Usage:"
echo "   - Run 'ai' to activate Ingest environment"
echo "   - Run 'ad' to activate dbt environment"
echo "   (Please restart your terminal or run 'source ~/.bashrc')"
echo "------------------------------------------------------------"
