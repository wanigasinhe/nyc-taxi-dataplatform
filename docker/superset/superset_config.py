import os

# ==============================================================================
# Apache Superset Configuration for NYC Taxi Data Platform
# ==============================================================================

ROW_LIMIT = 100000
SUPERSET_WEBSERVER_PORT = 8088
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "nyc_taxi_superset_secret_key_super_secure_12345")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/superset",
)

# App branding
APP_NAME = "NYC Taxi Analytics (Task 5 Superset)"
APP_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/d/d3/New_York_City_Taxi_logo.svg/320px-New_York_City_Taxi_logo.svg.png"

# Performance & Feature Flags
FEATURE_FLAGS = {
    "ALERT_REPORTS": False,
    "EMBEDDED_SUPERSET": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

# Security Configuration
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
HTTP_HEADERS = {}
