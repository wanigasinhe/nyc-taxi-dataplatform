#!/usr/bin/env bash
# ==============================================================================
# NYC Taxi Data Platform — Unified Infrastructure Entry Point (start.sh)
# Single script to bootstrap, start, stop, and monitor all platform services.
# Cross-Platform: macOS (Darwin) & Linux (Ubuntu/Debian/CentOS/RHEL)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"

# Load active environment configuration from settings/envs/ (single source of truth)
APP_ENV="${APP_ENV:-dev}"
ENV_FILE="settings/envs/${APP_ENV}.env"
if [ ! -f "${ENV_FILE}" ]; then
    ENV_FILE="settings/envs/dev.env"
fi
if [ -f "${ENV_FILE}" ]; then
    set -a
    source "${ENV_FILE}"
    set +a
fi

# ------------------------------------------------------------------------------
# Terminal Color Formatting
# ------------------------------------------------------------------------------
BOLD="\033[1m"
GREEN="\033[0;32m"
CYAN="\033[0;36m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------
log_info() { echo -e "${CYAN}[INFO]${RESET} $1"; }
log_success() { echo -e "${GREEN}[✔]${RESET} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${RESET} $1"; }
log_error() { echo -e "${RED}[ERROR]${RESET} $1"; }

get_docker_compose() {
    if docker compose version &>/dev/null; then
        echo "docker compose --env-file ${ENV_FILE}"
    elif command -v docker-compose &>/dev/null; then
        echo "docker-compose --env-file ${ENV_FILE}"
    else
        log_error "Neither 'docker compose' nor 'docker-compose' found. Please install Docker Compose."
        exit 1
    fi
}

DOCKER_COMPOSE="$(get_docker_compose)"

# ------------------------------------------------------------------------------
# Check Prerequisites & Bootstrap Environment
# ------------------------------------------------------------------------------
ensure_bootstrap() {
    log_info "Verifying platform bootstrap state..."

    # If setup.sh hasn't run or .venv / dev.env / data are missing, run setup.sh
    if [ ! -d ".venv" ] || [ ! -f "${ENV_FILE}" ] || [ ! -f "data/raw/taxi_zone_lookup.csv" ]; then
        log_warn "Initial environment setup incomplete. Executing bootstrap (setup.sh)..."
        chmod +x setup.sh
        ./setup.sh
    else
        log_success "Virtual environment, configuration (${ENV_FILE}), and raw datasets are present."
    fi
}

# ------------------------------------------------------------------------------
# Start Docker Infrastructure (PostgreSQL + Apache Airflow)
# ------------------------------------------------------------------------------
start_infrastructure() {
    log_info "Starting Docker infrastructure services..."

    # Check if Docker daemon is responsive
    if ! docker info &>/dev/null; then
        log_error "Docker daemon is not running. Please start Docker Desktop (macOS) or Docker Engine (Linux)."
        exit 1
    fi

    # 1. Start PostgreSQL
    log_info "Launching PostgreSQL 17 container..."
    ${DOCKER_COMPOSE} up -d postgres

    # 2. Await PostgreSQL Health
    log_info "Awaiting PostgreSQL health check on port 5432..."
    local MAX_RETRIES=25
    local COUNT=0
    local PG_HEALTHY=false

    until [ "${COUNT}" -ge "${MAX_RETRIES}" ]; do
        local STATUS
        STATUS=$(docker inspect --format='{{json .State.Health.Status}}' nyc_taxi_postgres 2>/dev/null || echo "\"starting\"")
        if [ "${STATUS}" = "\"healthy\"" ]; then
            PG_HEALTHY=true
            log_success "PostgreSQL is healthy and accepting connections on port 5432."
            break
        fi
        COUNT=$((COUNT + 1))
        sleep 2
    done

    if [ "${PG_HEALTHY}" = false ]; then
        log_warn "PostgreSQL did not report healthy within 50s. Check logs: ${DOCKER_COMPOSE} logs postgres"
    fi

    # 2.5 Ensure 'airflow' database exists in PostgreSQL
    log_info "Ensuring 'airflow' database exists in PostgreSQL..."
    docker exec nyc_taxi_postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'airflow'" | grep -q 1 || \
    docker exec nyc_taxi_postgres psql -U postgres -c "CREATE DATABASE airflow;"
    log_success "Database 'airflow' verified in PostgreSQL."

    # 3. Start Apache Airflow (Init -> Webserver -> Scheduler)
    log_info "Initializing Apache Airflow database and credentials..."
    ${DOCKER_COMPOSE} up -d airflow-init
    
    # Wait for airflow-init to complete
    log_info "Awaiting Airflow database migration..."
    local INIT_RETRIES=35
    local INIT_COUNT=0
    until [ "${INIT_COUNT}" -ge "${INIT_RETRIES}" ]; do
        local INIT_STATUS
        INIT_STATUS=$(docker inspect --format='{{.State.Status}}' nyc_taxi_airflow_init 2>/dev/null || echo "running")
        if [ "${INIT_STATUS}" = "exited" ]; then
            local EXIT_CODE
            EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' nyc_taxi_airflow_init 2>/dev/null || echo "0")
            if [ "${EXIT_CODE}" -ne 0 ]; then
                log_error "Airflow init failed with exit code ${EXIT_CODE}. Diagnostic logs:"
                ${DOCKER_COMPOSE} logs --tail=30 airflow-init
                exit 1
            fi
            log_success "Airflow database migrations and user provisioning complete."
            break
        fi
        INIT_COUNT=$((INIT_COUNT + 1))
        sleep 2
    done

    log_info "Starting Airflow Webserver, Scheduler, pgAdmin 4 & Apache Superset..."
    ${DOCKER_COMPOSE} up -d airflow-webserver airflow-scheduler pgadmin superset

    # Ensure admin user credentials explicitly
    log_info "Verifying Airflow admin credentials..."
    sleep 4
    docker exec nyc_taxi_airflow_webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin 2>/dev/null || \
    docker exec nyc_taxi_airflow_webserver airflow users reset-password \
        --username admin \
        --password admin 2>/dev/null || true

    log_success "Apache Airflow services running with credentials: admin / admin"
}

# ------------------------------------------------------------------------------
# Start Dashboard (Streamlit)
# ------------------------------------------------------------------------------
start_dashboard() {
    log_info "Configuring Streamlit Analytics Dashboard on port 8501..."
    mkdir -p logs

    # Stop any existing Streamlit process to reload latest application code
    local ST_PID
    ST_PID=$(lsof -ti :8501 2>/dev/null || true)
    if [ -n "${ST_PID}" ]; then
        log_info "Reloading existing Streamlit process (PID ${ST_PID})..."
        kill -9 ${ST_PID} 2>/dev/null || true
        sleep 1
    fi
    rm -f .streamlit.pid

    log_info "Launching Streamlit Dashboard in the background..."
    # Activate .venv and launch headless Streamlit with explicit PYTHONPATH
    # shellcheck source=/dev/null
    source .venv/bin/activate
    export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
    nohup streamlit run dashboard/app.py \
        --server.port 8501 \
        --server.headless true \
        --browser.serverAddress localhost \
        > logs/streamlit.log 2>&1 &
    echo $! > .streamlit.pid
    sleep 3
    log_success "Streamlit Dashboard started (PID: $(cat .streamlit.pid))."
}

# ------------------------------------------------------------------------------
# Status Check
# ------------------------------------------------------------------------------
check_status() {
    echo ""
    echo -e "${BOLD}==================================================================${RESET}"
    echo -e "${BOLD}📡 NYC Taxi Data Platform — Service Status${RESET}"
    echo -e "${BOLD}==================================================================${RESET}"

    # Check Docker Services
    echo -e "\n${BOLD}Container Services:${RESET}"
    ${DOCKER_COMPOSE} ps

    # Check Streamlit Dashboard
    echo -e "\n${BOLD}Streamlit Dashboard:${RESET}"
    if lsof -i :8501 &>/dev/null; then
        echo -e "  Streamlit: ${GREEN}RUNNING${RESET} (Listening on http://localhost:8501)"
    else
        echo -e "  Streamlit: ${YELLOW}STOPPED${RESET}"
    fi

    # Check Raw Data Files
    echo -e "\n${BOLD}Raw Data Ingestion Cache:${RESET}"
    for f in data/raw/*; do
        if [ -f "${f}" ]; then
            echo -e "  - $(basename "${f}") ($(.venv/bin/python -c "import os; print(f'{os.path.getsize(\"${f}\") / (1024*1024):.2f} MB')" 2>/dev/null || echo "present"))"
        fi
    done
    echo ""
}

# ------------------------------------------------------------------------------
# Stop Platform Services
# ------------------------------------------------------------------------------
stop_platform() {
    log_info "Stopping NYC Taxi Data Platform..."

    # Stop Streamlit
    if [ -f ".streamlit.pid" ]; then
        local PID
        PID=$(cat .streamlit.pid)
        log_info "Stopping Streamlit process (PID ${PID})..."
        kill "${PID}" 2>/dev/null || true
        rm -f .streamlit.pid
    fi

    # Kill any dangling streamlit process on 8501
    local ST_PID
    ST_PID=$(lsof -ti :8501 2>/dev/null || true)
    if [ -n "${ST_PID}" ]; then
        kill -9 ${ST_PID} 2>/dev/null || true
    fi

    # Stop Docker Compose Services
    log_info "Shutting down Docker containers..."
    ${DOCKER_COMPOSE} down
    log_success "All platform services stopped cleanly."
}

# ------------------------------------------------------------------------------
# Trigger Batch ETL Pipeline (via Apache Airflow or Python fallback)
# ------------------------------------------------------------------------------
run_etl() {
    log_info "Triggering NYC Taxi ETL Pipeline..."
    export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"

    # shellcheck source=/dev/null
    source .venv/bin/activate

    log_info "Active Data Configuration (loaded from settings):"
    python -c "
from settings import settings
d = settings.data
print(f'  - Environment:    {settings.ENVIRONMENT}')
print(f'  - RAW_DATA_URL:   {d.RAW_DATA_URL or d.DEFAULT_TLC_BASE_URL}')
print(f'  - Timeframe:      {d.get_overall_timeframe()[0]} -> {d.get_overall_timeframe()[1]}')
print(f'  - Target Months:  {d.target_months}')
if d.START_DATE:
    print(f'  - Exact Filter:   {d.START_DATE} -> {d.END_DATE or \"open\"}')
print(f'  - DAY_CHUNK_SIZE: {d.DAY_CHUNK_SIZE} days per slice (e.g. 1-10 then finish, 11-20 then finish)')
print(f'  - Memory Chunk:   {d.BATCH_CHUNK_SIZE:,} rows per PyArrow batch')
" 2>/dev/null || true

    # If explicit date/month filters or DIRECT mode are passed, execute direct Python runner
    if [ "${DIRECT:-0}" = "1" ] || [ -n "${START_DATE:-}" ] || [ -n "${START_MONTH:-}" ] || [ -n "${RAW_DATA_URL:-}" ]; then
        log_info "Executing pipeline via direct Python runner with active data settings..."
        python -m src.batch_etl.flow
        log_success "Batch ETL flow complete."
    elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "nyc_taxi_airflow_webserver"; then
        log_info "Triggering DAG 'nyc_taxi_batch_pipeline' inside Apache Airflow..."
        docker exec nyc_taxi_airflow_webserver airflow dags trigger nyc_taxi_batch_pipeline
        log_success "DAG 'nyc_taxi_batch_pipeline' triggered successfully in Apache Airflow."
        log_info "Monitor execution progress, logs, and graph view at: http://localhost:8080"
    else
        log_warn "Airflow container is not running. Executing pipeline via direct Python runner..."
        python -m src.batch_etl.flow
        log_success "Batch ETL flow complete."
    fi
}

# ------------------------------------------------------------------------------
# Print Service Access Banner
# ------------------------------------------------------------------------------
print_banner() {
    echo ""
    echo -e "${BOLD}==================================================================${RESET}"
    echo -e "${GREEN}${BOLD}🚀 All Platform Services Started Successfully!${RESET}"
    echo -e "${BOLD}==================================================================${RESET}"
    echo -e "Access your endpoints below:"
    echo ""
    echo -e "  ${BOLD}🌬️  Apache Airflow UI (All Job Control & Monitoring):${RESET} ${CYAN}http://localhost:8080${RESET}"
    echo -e "     - Username: ${BOLD}admin${RESET} | Password: ${BOLD}admin${RESET}"
    echo -e "     - DAG: ${BOLD}nyc_taxi_batch_pipeline${RESET}"
    echo -e "     - Handles: DAG execution, task-level logs, retries, graph/grid monitoring"
    echo ""
    echo -e "  ${BOLD}📊 Streamlit Dashboard (Business Analytics):${RESET}        ${CYAN}http://localhost:8501${RESET}"
    echo -e "     - Executive KPIs, Fare/Mile, Peak Hours, Route Corridors"
    echo -e "     - ${BOLD}🔍 Tab 6: Interactive PostgreSQL Query Window${RESET} (Real-time SQL Console)"
    echo ""
    echo -e "  ${BOLD}🌐 pgAdmin 4 (PostgreSQL Web IDE & Query Tool):${RESET}      ${CYAN}http://localhost:5050${RESET}"
    echo -e "     - Login: ${BOLD}admin@admin.com${RESET} | Password: ${BOLD}admin${RESET} (Auto-Login Bypassed)"
    echo -e "     - Pre-connected to ${BOLD}nyc_taxi${RESET} (Warehouse) & ${BOLD}airflow${RESET} (Metadata)"
    echo -e "     - Full SQL Query Tool, Explain Visualizer, Table Editor, ER Diagrams"
    echo ""
    echo -e "  ${BOLD}🏛️  Apache Superset Dashboard (Task 5 Enterprise BI):${RESET} ${CYAN}http://localhost:8088${RESET}"
    echo -e "     - Login: ${BOLD}admin${RESET} | Password: ${BOLD}admin${RESET}"
    echo -e "     - Pre-built Dashboard: ${BOLD}http://localhost:8088/superset/dashboard/nyc-taxi-task5/${RESET}"
    echo -e "     - Pre-configured with Task 5 Datasets: Fare/Mile, Peak Hours & Payment Revenue"
    echo ""
    echo -e "  ${BOLD}🐘 PostgreSQL 17 Database:${RESET} ${CYAN}localhost:5432${RESET}"
    echo -e "     - Database: ${BOLD}nyc_taxi${RESET} (Star Schema), ${BOLD}airflow${RESET} (Metadata)"
    echo -e "     - User: ${BOLD}postgres${RESET} | Password: ${BOLD}postgres${RESET}"
    echo ""
    echo -e "  ${BOLD}📜 Telemetry Metrics Log:${RESET}  ${CYAN}logs/etl_metrics.jsonl${RESET}"
    echo -e "  ${BOLD}📅 Daily Processed Logs:${RESET}    ${CYAN}logs/daily_processed_metrics.jsonl${RESET}"
    echo -e "  ${BOLD}📦 Parquet File Metrics:${RESET}    ${CYAN}logs/parquet_file_metrics.jsonl${RESET}"
    echo ""
    echo -e "${BOLD}Helpful Commands:${RESET}"
    echo -e "  Check status:      ${YELLOW}./start.sh status${RESET}"
    echo -e "  Open query window: ${YELLOW}./start.sh query${RESET}"
    echo -e "  Run ETL directly:  ${YELLOW}./start.sh etl${RESET}"
    echo -e "  Stop everything:   ${YELLOW}./start.sh stop${RESET}"
    echo -e "${BOLD}==================================================================${RESET}"
    echo ""
}

# ------------------------------------------------------------------------------
# CLI Dispatcher
# ------------------------------------------------------------------------------
COMMAND="${1:-all}"

case "${COMMAND}" in
    all|"")
        ensure_bootstrap
        start_infrastructure
        start_dashboard
        print_banner
        ;;
    infra)
        ensure_bootstrap
        start_infrastructure
        log_success "Docker infrastructure running. Dashboard not started."
        ;;
    dashboard)
        ensure_bootstrap
        start_dashboard
        log_success "Streamlit dashboard running on http://localhost:8501"
        ;;
    pgadmin)
        log_info "Ensuring PostgreSQL & pgAdmin 4 are started..."
        ${DOCKER_COMPOSE} up -d postgres pgadmin
        log_success "pgAdmin 4 running at: http://localhost:5050 (Login: admin@admin.com / admin)"
        ;;
    superset)
        ensure_bootstrap
        ${DOCKER_COMPOSE} up -d postgres superset
        log_success "Apache Superset running at: http://localhost:8088/superset/dashboard/nyc-taxi-task5/ (Login: admin / admin)"
        ;;
    query)
        ensure_bootstrap
        start_dashboard
        log_success "PostgreSQL Query Window available at: http://localhost:8501 (Tab 6: PostgreSQL Query Window)"
        ;;
    etl|run-etl)
        ensure_bootstrap
        run_etl
        ;;
    slack-test|test-slack)
        ensure_bootstrap
        log_info "Testing Slack Alerting System..."
        .venv/bin/python -m src.alerts.slack --test
        ;;
    fix-admin|reset-admin)
        log_info "Ensuring 'airflow' database and provisioning admin / admin credentials..."
        ${DOCKER_COMPOSE} up -d postgres
        sleep 2
        docker exec nyc_taxi_postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'airflow'" 2>/dev/null | grep -q 1 || \
        docker exec nyc_taxi_postgres psql -U postgres -c "CREATE DATABASE airflow;" 2>/dev/null || true
        ${DOCKER_COMPOSE} up -d airflow-init
        ${DOCKER_COMPOSE} up -d airflow-webserver airflow-scheduler
        sleep 5
        docker exec nyc_taxi_airflow_webserver airflow users create \
            --username admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com \
            --password admin 2>/dev/null || \
        docker exec nyc_taxi_airflow_webserver airflow users reset-password \
            --username admin \
            --password admin 2>/dev/null || true
        log_success "Admin credentials set successfully! Username: admin | Password: admin"
        ;;
    status)
        check_status
        ;;
    stop|down)
        stop_platform
        ;;
    restart)
        stop_platform
        sleep 2
        ensure_bootstrap
        start_infrastructure
        start_dashboard
        print_banner
        ;;
    *)
        echo "Usage: $0 {all|infra|dashboard|etl|test-slack|status|stop|restart}"
        echo ""
        echo "Commands:"
        echo "  all        Start everything: Postgres, Airflow, and Streamlit Dashboard (default)"
        echo "  infra      Start Docker infrastructure only (PostgreSQL + Airflow)"
        echo "  dashboard  Start/restart the Streamlit Dashboard only"
        echo "  etl        Run the batch ETL pipeline directly via Python"
        echo "  test-slack Send a test alert to verify the Slack Webhook integration"
        echo "  status     Show status of all platform services, ports, and data files"
        echo "  stop       Stop all containers and background processes cleanly"
        echo "  restart    Restart the entire platform"
        exit 1
        ;;
esac
