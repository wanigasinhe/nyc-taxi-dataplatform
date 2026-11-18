"""Script to provision Task 5 Analytics Dashboard and Charts inside Superset."""

import json
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset import db

    print("Checking Superset datasets...")
    t_fare = db.session.query(SqlaTable).filter_by(table_name="view_fare_per_mile_by_borough_rate").first()
    t_peak = db.session.query(SqlaTable).filter_by(table_name="view_peak_hours_analysis").first()
    t_pay = db.session.query(SqlaTable).filter_by(table_name="view_revenue_by_payment_type").first()
    t_kpi = db.session.query(SqlaTable).filter_by(table_name="view_kpi_summary").first()

    if not all([t_fare, t_peak, t_pay, t_kpi]):
        print("Missing some tables. Available:", [(t.id, t.table_name) for t in db.session.query(SqlaTable).all()])
        exit(1)

    print("Creating/updating Slices...")
    # 1. Executive KPIs
    s_kpi = db.session.query(Slice).filter_by(slice_name="Executive KPI Summary").first()
    kpi_params = {
        "datasource": f"{t_kpi.id}__table",
        "viz_type": "table",
        "all_columns": [
            "total_trips",
            "total_passengers",
            "total_revenue_usd",
            "total_distance_miles",
            "weighted_avg_fare_per_mile",
            "avg_duration_minutes",
            "credit_card_tip_pct",
        ],
        "order_by_cols": [],
        "server_page_length": 10,
    }
    if not s_kpi:
        s_kpi = Slice(
            slice_name="Executive KPI Summary",
            viz_type="table",
            datasource_type="table",
            datasource_id=t_kpi.id,
            params=json.dumps(kpi_params),
        )
        db.session.add(s_kpi)
    else:
        s_kpi.params = json.dumps(kpi_params)

    # 2. Task 5.1: Average Fare Amount per Mile
    s_fare = db.session.query(Slice).filter_by(slice_name="Task 5.1: Average Fare Amount per Mile by Borough").first()
    fare_params = {
        "datasource": f"{t_fare.id}__table",
        "viz_type": "table",
        "all_columns": [
            "pickup_borough",
            "rate_description",
            "trip_count",
            "total_distance",
            "total_fare_amount",
            "weighted_avg_fare_per_mile",
        ],
        "order_by_cols": ['["trip_count", false]'],
        "server_page_length": 25,
    }
    if not s_fare:
        s_fare = Slice(
            slice_name="Task 5.1: Average Fare Amount per Mile by Borough",
            viz_type="table",
            datasource_type="table",
            datasource_id=t_fare.id,
            params=json.dumps(fare_params),
        )
        db.session.add(s_fare)
    else:
        s_fare.params = json.dumps(fare_params)

    # 3. Task 5.2: Peak Hours for Taxi Rides
    s_peak = db.session.query(Slice).filter_by(slice_name="Task 5.2: 24h Peak Hours Analysis (Weekday vs Weekend)").first()
    peak_params = {
        "datasource": f"{t_peak.id}__table",
        "viz_type": "table",
        "all_columns": [
            "pickup_hour",
            "day_category",
            "total_trips",
            "avg_duration_min",
            "avg_distance_miles",
            "total_revenue_usd",
            "avg_fare_usd",
        ],
        "order_by_cols": ['["pickup_hour", true]', '["day_category", true]'],
        "server_page_length": 50,
    }
    if not s_peak:
        s_peak = Slice(
            slice_name="Task 5.2: 24h Peak Hours Analysis (Weekday vs Weekend)",
            viz_type="table",
            datasource_type="table",
            datasource_id=t_peak.id,
            params=json.dumps(peak_params),
        )
        db.session.add(s_peak)
    else:
        s_peak.params = json.dumps(peak_params)

    # 4. Task 5.3: Revenue by Payment Type
    s_pay = db.session.query(Slice).filter_by(slice_name="Task 5.3: Total Revenue and Tips by Payment Type").first()
    pay_params = {
        "datasource": f"{t_pay.id}__table",
        "viz_type": "table",
        "all_columns": [
            "payment_name",
            "trip_count",
            "total_revenue_usd",
            "total_tips_usd",
            "tip_pct_of_fare",
            "revenue_share_pct",
        ],
        "order_by_cols": ['["total_revenue_usd", false]'],
        "server_page_length": 10,
    }
    if not s_pay:
        s_pay = Slice(
            slice_name="Task 5.3: Total Revenue and Tips by Payment Type",
            viz_type="table",
            datasource_type="table",
            datasource_id=t_pay.id,
            params=json.dumps(pay_params),
        )
        db.session.add(s_pay)
    else:
        s_pay.params = json.dumps(pay_params)

    db.session.commit()
    print("✔ Slices created/updated successfully!")

    # Create Dashboard
    dash_title = "NYC Yellow Taxi Analytics Dashboard (Task 5)"
    dash = db.session.query(Dashboard).filter_by(dashboard_title=dash_title).first()
    if not dash:
        dash = Dashboard(
            dashboard_title=dash_title,
            slug="nyc-taxi-task5",
            published=True,
        )
        db.session.add(dash)
        db.session.commit()
        print("✔ Created Dashboard:", dash_title)
    else:
        print("✔ Dashboard already exists:", dash_title)

    dash.slices = [s_kpi, s_fare, s_peak, s_pay]
    db.session.commit()
    print(f"✔ Assigned {len(dash.slices)} charts to dashboard '{dash_title}' (Slug: /superset/dashboard/nyc-taxi-task5/)!")
