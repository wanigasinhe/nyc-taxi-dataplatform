#!/usr/bin/env bash
# ==============================================================================
# NYC Taxi Data Platform — Automated Idempotent Setup Script (Task 1)
# Cross-Platform: Linux (Ubuntu/Debian/RHEL/CentOS) & macOS (Darwin)
# ==============================================================================
set -euo pipefail

# Ensure script runs from the repository root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"

echo "=================================================================="
echo "🚕 Bootstrapping NYC Taxi Data Platform — Task 1: Environment & Setup"
echo "=================================================================="

# ------------------------------------------------------------------------------
# 1. Dependency & Prerequisite Checks
# ------------------------------------------------------------------------------
echo "🔍 Checking system prerequisites..."

# 1.1 Detect Python 3.10+
PYTHON_BIN=""
for cmd in python3.12 python3.11 python3.10 python3; do
    if command -v "${cmd}" &>/dev/null; then
        PY_VER=$("${cmd}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        PY_MAJOR=$(echo "${PY_VER}" | cut -d. -f1)
        PY_MINOR=$(echo "${PY_VER}" | cut -d. -f2)
        if [ "${PY_MAJOR}" -eq 3 ] && [ "${PY_MINOR}" -ge 10 ]; then
            PYTHON_BIN="${cmd}"
            echo "  ✔ Python ${PY_VER} found: $(command -v "${cmd}")"
            break
        fi
    fi
done

if [ -z "${PYTHON_BIN}" ]; then
    echo "❌ Error: Python 3.10 or higher is required. Please install Python 3.10, 3.11, or 3.12."
    exit 1
fi

# 1.2 Check Docker CLI
if ! command -v docker &>/dev/null; then
    echo "❌ Error: docker is not installed. Please install Docker Engine or Docker Desktop."
    exit 1
fi
echo "  ✔ Docker CLI found: $(docker --version)"

# 1.3 Check Docker Compose (v2 plugin or v1 standalone)
DOCKER_COMPOSE_CMD=""
if docker compose version &>/dev/null; then
    DOCKER_COMPOSE_CMD="docker compose --env-file settings/envs/dev.env"
elif command -v docker-compose &>/dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose --env-file settings/envs/dev.env"
else
    echo "❌ Error: Docker Compose is required. Please install 'docker compose'."
    exit 1
fi
echo "  ✔ Docker Compose found: ${DOCKER_COMPOSE_CMD}"

# 1.4 Check Download Utility (curl or wget)
DOWNLOAD_TOOL=""
if command -v curl &>/dev/null; then
    DOWNLOAD_TOOL="curl"
    echo "  ✔ Download utility found: curl"
elif command -v wget &>/dev/null; then
    DOWNLOAD_TOOL="wget"
    echo "  ✔ Download utility found: wget"
else
    echo "❌ Error: Neither 'curl' nor 'wget' was found. Please install curl or wget."
    exit 1
fi

# ------------------------------------------------------------------------------
# 2. Virtual Environment Setup & Dependencies Installation
# ------------------------------------------------------------------------------
echo ""
echo "📦 Setting up Python virtual environment (.venv)..."
if [ ! -d ".venv" ]; then
    "${PYTHON_BIN}" -m venv .venv
    echo "  ✔ Created virtual environment at .venv using ${PYTHON_BIN}"
else
    echo "  ✔ Existing .venv detected (skipping recreation)"
fi

# Activate virtual environment
# shellcheck source=/dev/null
source .venv/bin/activate

echo "  ⬆ Upgrading pip and installing pinned dependencies from requirements.txt..."
pip install --upgrade pip > /dev/null
pip install -r requirements.txt > /dev/null
echo "  ✔ All Python dependencies installed successfully."

# ------------------------------------------------------------------------------
# 3. Environment Configuration
# ------------------------------------------------------------------------------
if [ ! -f "settings/envs/dev.env" ]; then
    echo "❌ Error: settings/envs/dev.env not found. Please ensure settings package is present."
    exit 1
else
    echo "  ✔ Configuration verified at settings/envs/dev.env"
fi

# ------------------------------------------------------------------------------
# 4. PostgreSQL 17 Docker Container Startup & Health Verification
# ------------------------------------------------------------------------------
echo ""
echo "🐘 Starting PostgreSQL 17 database container..."

# Check if Docker daemon is responsive
if ! docker info &>/dev/null; then
    echo "⚠️ Warning: Docker daemon is not running or not accessible."
    echo "  - On macOS: Start Docker Desktop."
    echo "  - On Linux: Run 'sudo systemctl start docker'."
    echo "After starting Docker, re-run ./setup.sh or run '${DOCKER_COMPOSE_CMD} up -d postgres'."
else
    ${DOCKER_COMPOSE_CMD} up -d postgres

    echo "  ⏳ Awaiting PostgreSQL health check on port 5432..."
    MAX_RETRIES=25
    COUNT=0
    HEALTHY=false

    until [ "${COUNT}" -ge "${MAX_RETRIES}" ]; do
        STATUS=$(docker inspect --format='{{json .State.Health.Status}}' nyc_taxi_postgres 2>/dev/null || echo "\"starting\"")
        if [ "${STATUS}" = "\"healthy\"" ]; then
            echo "  ✔ PostgreSQL 17 is healthy and accepting connections on port 5432."
            HEALTHY=true
            break
        fi
        COUNT=$((COUNT + 1))
        sleep 2
    done

    if [ "${HEALTHY}" = false ]; then
        echo "⚠️ Notice: PostgreSQL container did not report healthy within 50s."
        echo "Check container status with: ${DOCKER_COMPOSE_CMD} logs postgres"
    fi
fi

# ------------------------------------------------------------------------------
# 5. Programmatic Download of NYC Taxi Data (curl / wget)
# ------------------------------------------------------------------------------
echo ""
echo "📥 Ensuring raw NYC Taxi datasets are present in data/raw/..."
mkdir -p data/raw logs

# Helper download function supporting curl or wget with retries & integrity check
download_file() {
    local url="$1"
    local dest="$2"

    if [ -f "${dest}" ] && [ -s "${dest}" ]; then
        echo "  ✔ $(basename "${dest}") already exists (skipping download)"
        return 0
    fi

    echo "  ⬇ Downloading $(basename "${dest}")..."
    if [ "${DOWNLOAD_TOOL}" = "curl" ]; then
        curl -fSL --retry 3 --connect-timeout 15 "${url}" -o "${dest}"
    else
        wget -q --tries=3 --timeout=15 "${url}" -O "${dest}"
    fi

    # Verify non-empty
    if [ ! -s "${dest}" ]; then
        echo "❌ Error: Downloaded file $(basename "${dest}") is empty."
        rm -f "${dest}"
        exit 1
    fi

    echo "  ✔ Downloaded and verified $(basename "${dest}") ($("${PYTHON_BIN}" -c "import os; print(f'{os.path.getsize(\"${dest}\") / (1024*1024):.2f} MB')"))"
}

# 5.1 Download Official NYC TLC Taxi Zone Lookup CSV
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_DEST="data/raw/taxi_zone_lookup.csv"
download_file "${ZONE_URL}" "${ZONE_DEST}"

# 5.2 Download NYC Yellow Taxi Parquet Files First (for configured date range)
echo "  Downloading Parquet files first for configured date range..."
TARGET_MONTHS=($("${PYTHON_BIN}" -c "from settings import settings; print(' '.join(settings.target_months))"))
echo "  Target months for batch ingestion: ${TARGET_MONTHS[*]}"
for m in "${TARGET_MONTHS[@]}"; do
    PARQUET_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${m}.parquet"
    PARQUET_DEST="data/raw/yellow_tripdata_${m}.parquet"
    download_file "${PARQUET_URL}" "${PARQUET_DEST}"
done

echo ""
echo "=================================================================="
echo "🎉 Task 1 Bootstrap Completed Successfully!"
echo "=================================================================="
echo "Next Steps to run the platform:"
echo ""
echo "  Option A: Orchestrate with Apache Airflow (Docker UI):"
echo "     docker compose up -d airflow-webserver airflow-scheduler"
echo "     Access Web UI at: http://localhost:8080 (User: admin / Pass: admin)"
echo ""
echo "  Option B: Execute Pipeline Directly in Python:"
echo "     source .venv/bin/activate"
echo "     python -m src.batch_etl.flow"
echo ""
echo "  Launch the Analytics & Observability Dashboard (Task 5):"
echo "     source .venv/bin/activate"
echo "     streamlit run dashboard/app.py (UI at http://localhost:8501)"
echo "=================================================================="
