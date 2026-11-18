#!/bin/bash
set -e

echo "=========================================================="
echo "🚀 Initializing Apache Superset for NYC Taxi Data Platform"
echo "=========================================================="

echo "1. Running Superset database migrations..."
superset db upgrade

echo "2. Creating admin user (admin / admin)..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@admin.com \
  --password admin 2>/dev/null || true

echo "3. Initializing Superset roles and permissions..."
superset init

echo "4. Auto-registering NYC Taxi PostgreSQL database connection & Task 5 datasets..."
python -c "
import os
from superset.app import create_app
app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    from superset import db

    db_name = 'NYC Taxi Platform'
    existing = db.session.query(Database).filter_by(database_name=db_name).first()
    db_uri = 'postgresql+psycopg2://postgres:postgres@postgres:5432/nyc_taxi'
    
    if not existing:
        d = Database(
            database_name=db_name,
            sqlalchemy_uri=db_uri,
            expose_in_sqllab=True,
            allow_run_async=True,
            allow_ctas=True,
            allow_cvas=True,
            allow_dml=False
        )
        db.session.add(d)
        db.session.commit()
        print('✔ Added database connection:', db_name)
    else:
        d = existing
        print('✔ Database connection already exists:', db_name)

    # Register analytical datasets for Task 5
    tables = [
        ('view_fare_per_mile_by_borough_rate', 'Task 5.1: Fare per Mile by Borough & Rate'),
        ('view_peak_hours_analysis', 'Task 5.2: 24h Peak Hours Analysis'),
        ('view_revenue_by_payment_type', 'Task 5.3: Revenue by Payment Type'),
        ('view_kpi_summary', 'Executive Summary KPIs'),
        ('view_top_corridors', 'Top 50 Route Corridors')
    ]
    for tbl_name, verbose_name in tables:
        t_exist = db.session.query(SqlaTable).filter_by(table_name=tbl_name, database_id=d.id).first()
        if not t_exist:
            tbl = SqlaTable(table_name=tbl_name, schema='public', database=d, description=verbose_name)
            try:
                tbl.fetch_metadata()
                db.session.add(tbl)
                db.session.commit()
                print(f'✔ Registered dataset: {tbl_name}')
            except Exception as e:
                print(f'Notice registering {tbl_name}: {e}')
" || true

echo "=========================================================="
echo "✔ Apache Superset initialization completed successfully!"
echo "=========================================================="
