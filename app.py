import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, Date
from sqlalchemy import insert
from datetime import datetime
from sqlalchemy import text

# Replace with your actual credentials
host = 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com'
user = '3qKZvyc8Bw7Ckf1.root'
password = 'ixPIapSBo4owm2Qf'
database = 'Asteroids'


# Create SQLAlchemy engine
# Reconnect to the Asteroidsnew DB
engine = sqlalchemy.create_engine("mysql+mysqlconnector://3qKZvyc8Bw7Ckf1.root:ixPIapSBo4owm2Qf@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/Asteroids")

metadata = MetaData()

from sqlalchemy import create_engine

# Connect to the database (replace with your own credentials)
engine = sqlalchemy.create_engine("mysql+mysqlconnector://3qKZvyc8Bw7Ckf1.root:ixPIapSBo4owm2Qf@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/Asteroids")

st.set_page_config(page_title="NASA Asteroid Dashboard", layout="wide")
st.title("🚀 NASA Near-Earth Object (NEO) Insights")
# Dropdown for selecting query
query_options = [
    "1. Count how many times each asteroid has approached Earth",
    "2. Average velocity of each asteroid",
    "3. Top 10 fastest asteroids",
    "4. Hazardous asteroids that approached > 3 times",
    "5. Month with most asteroid approaches",
    "6. Fastest ever approach",
    "7. Sort asteroids by max diameter",
    "8. Asteroids getting closer over time",
    "9. Closest approach per asteroid",
    "10. Velocity > 50,000 km/h",
    "11. Approach count per month",
    "12. Brightest asteroid",
    "13. Hazardous vs Non-Hazardous count",
    "14. Passed closer than 1 lunar distance",
    "15. Passed within 0.05 AU"
]

selected_query = st.selectbox("Select a query to run:", query_options)

with engine.connect() as conn:
    if selected_query.startswith("1"):
        result = conn.execute(text("""
            SELECT neo_reference_id, COUNT(*) AS approach_count
            FROM close_approach
            GROUP BY neo_reference_id
            ORDER BY approach_count DESC;
        """)).fetchall()

    elif selected_query.startswith("2"):
        result = conn.execute(text("""
            SELECT neo_reference_id, AVG(relative_velocity_kmph) AS avg_velocity
            FROM close_approach
            GROUP BY neo_reference_id
            ORDER BY avg_velocity DESC;
        """)).fetchall()

    elif selected_query.startswith("3"):
        result = conn.execute(text("""
            SELECT neo_reference_id, MAX(relative_velocity_kmph) AS max_velocity
            FROM close_approach
            GROUP BY neo_reference_id
            ORDER BY max_velocity DESC
            LIMIT 10;
        """)).fetchall()

    elif selected_query.startswith("4"):
        result = conn.execute(text("""
            SELECT a.name, ca.neo_reference_id, COUNT(*) AS approach_count
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            WHERE a.hazardous_asteroid = TRUE
            GROUP BY ca.neo_reference_id, a.name
            HAVING approach_count > 3;
        """)).fetchall()

    elif selected_query.startswith("5"):
        result = conn.execute(text("""
            SELECT MONTH(close_approach_date) AS month, COUNT(*) AS total_approaches
            FROM close_approach
            GROUP BY month
            ORDER BY total_approaches DESC
            LIMIT 1;
        """)).fetchall()

    elif selected_query.startswith("6"):
        result = conn.execute(text("""
            SELECT a.name, ca.relative_velocity_kmph
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            ORDER BY ca.relative_velocity_kmph DESC
            LIMIT 1;
        """)).fetchall()

    elif selected_query.startswith("7"):
        result = conn.execute(text("""
            SELECT name, estimated_diameter_max_km
            FROM asteroids
            ORDER BY estimated_diameter_max_km DESC;
        """)).fetchall()

    elif selected_query.startswith("8"):
        df = pd.read_sql(text("""
            SELECT neo_reference_id, close_approach_date, miss_distance_km
            FROM close_approach
            ORDER BY neo_reference_id, close_approach_date;
        """), conn)

        def is_decreasing(series):
            return all(x > y for x, y in zip(series, series[1:]))

        df_filtered = df.groupby("neo_reference_id").filter(
            lambda group: len(group) > 1 and is_decreasing(group["miss_distance_km"])
        )
        result = df_filtered.to_records(index=False)

    elif selected_query.startswith("9"):
        result = conn.execute(text("""
            SELECT a.name, ca.close_approach_date, ca.miss_distance_km
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            ORDER BY ca.miss_distance_km ASC;
        """)).fetchall()

    elif selected_query.startswith("10"):
        result = conn.execute(text("""
            SELECT a.name, ca.relative_velocity_kmph
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            WHERE ca.relative_velocity_kmph > 50000;
        """)).fetchall()

    elif selected_query.startswith("11"):
        result = conn.execute(text("""
            SELECT MONTH(close_approach_date) AS month, COUNT(*) AS total_approaches
            FROM close_approach
            GROUP BY month
            ORDER BY month;
        """)).fetchall()

    elif selected_query.startswith("12"):
        result = conn.execute(text("""
            SELECT name, magnitude
            FROM asteroids
            ORDER BY magnitude ASC
            LIMIT 1;
        """)).fetchall()

    elif selected_query.startswith("13"):
        result = conn.execute(text("""
            SELECT hazardous_asteroid, COUNT(*) AS count
            FROM asteroids
            GROUP BY hazardous_asteroid;
        """)).fetchall()

    elif selected_query.startswith("14"):
        result = conn.execute(text("""
            SELECT a.name, ca.close_approach_date, ca.miss_distance_lunar
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            WHERE ca.miss_distance_lunar < 1.0;
        """)).fetchall()

    elif selected_query.startswith("15"):
        result = conn.execute(text("""
            SELECT a.name, ca.astronomical
            FROM close_approach ca
            JOIN asteroids a ON ca.neo_reference_id = a.id
            WHERE ca.astronomical < 0.05;
        """)).fetchall()

# Convert and display
if selected_query.startswith("8"):
    st.dataframe(df_filtered)
    selected_id = st.selectbox("Select an asteroid ID to plot", df_filtered["neo_reference_id"].unique())
    chart_data = df_filtered[df_filtered["neo_reference_id"] == selected_id].set_index("close_approach_date")
    st.line_chart(chart_data["miss_distance_km"])
else:
    if result:
        st.dataframe(pd.DataFrame(result))
    else:
        st.warning("No results found for this query.")
