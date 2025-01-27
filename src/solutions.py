import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection settings
db_settings = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}


def create_database_if_not_exists():
    """Create the database if it doesn't already exist."""
    conn = psycopg2.connect(
        host=db_settings["host"],
        port=db_settings["port"],
        user=db_settings["user"],
        password=db_settings["password"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_settings['database']}';")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"CREATE DATABASE {db_settings['database']};")
                print(f"Database '{db_settings['database']}' created successfully.")
            else:
                print(f"Database '{db_settings['database']}' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        conn.close()


def check_table_empty(conn, table_name):
    """Check if a table is empty."""
    query = f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);"
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
    return not result[0]  # True if the table is empty


def load_data_to_postgres(trip_data_path, zone_lookup_path):
    """Load CSV data into PostgreSQL tables."""
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            # Create green_tripdata table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS green_tripdata (
                    VendorID INTEGER,
                    lpep_pickup_datetime TIMESTAMP,
                    lpep_dropoff_datetime TIMESTAMP,
                    store_and_fwd_flag CHAR(1),
                    RatecodeID INTEGER,
                    PULocationID INTEGER,
                    DOLocationID INTEGER,
                    passenger_count INTEGER,
                    trip_distance FLOAT,
                    fare_amount FLOAT,
                    extra FLOAT,
                    mta_tax FLOAT,
                    tip_amount FLOAT,
                    tolls_amount FLOAT,
                    ehail_fee FLOAT,
                    improvement_surcharge FLOAT,
                    total_amount FLOAT,
                    payment_type INTEGER,
                    trip_type INTEGER,
                    congestion_surcharge FLOAT
                );
            """)
            conn.commit()

            # Check if green_tripdata table is empty
            if check_table_empty(conn, "green_tripdata"):
                with open(trip_data_path, 'r') as f:
                    cur.copy_expert(
                        "COPY green_tripdata FROM STDIN WITH CSV HEADER",
                        f
                    )
                conn.commit()
                print("Loaded data into green_tripdata.")
            else:
                print("green_tripdata table already contains data. Skipping load.")

            # Create taxi_zone_lookup table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS taxi_zone_lookup (
                    LocationID INTEGER PRIMARY KEY,
                    Borough TEXT,
                    Zone TEXT,
                    service_zone TEXT
                );
            """)
            conn.commit()

            # Check if taxi_zone_lookup table is empty
            if check_table_empty(conn, "taxi_zone_lookup"):
                with open(zone_lookup_path, 'r') as f:
                    cur.copy_expert(
                        "COPY taxi_zone_lookup FROM STDIN WITH CSV HEADER",
                        f
                    )
                conn.commit()
                print("Loaded data into taxi_zone_lookup.")
            else:
                print("taxi_zone_lookup table already contains data. Skipping load.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
    finally:
        conn.close()


def calculate_trip_segmentation_postgres():
    """Calculate trip segmentation counts using SQL."""
    query = """
        SELECT
            COUNT(*) FILTER (WHERE trip_distance <= 1) AS short_trips,
            COUNT(*) FILTER (WHERE trip_distance > 1 AND trip_distance <= 3) AS medium_trips,
            COUNT(*) FILTER (WHERE trip_distance > 3 AND trip_distance <= 7) AS long_trips,
            COUNT(*) FILTER (WHERE trip_distance > 7 AND trip_distance <= 10) AS extra_long_trips,
            COUNT(*) FILTER (WHERE trip_distance > 10) AS very_long_trips
        FROM green_tripdata;
    """
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return {
            "Short Trips (≤ 1 mile)": result[0],
            "Medium Trips (1–3 miles)": result[1],
            "Long Trips (3–7 miles)": result[2],
            "Extra Long Trips (7–10 miles)": result[3],
            "Very Long Trips (> 10 miles)": result[4],
            "Answer Format": f"{result[0]}; {result[1]}; {result[2]}; {result[3]}; {result[4]}"
        }
    except Exception as e:
        print(f"Error calculating trip segmentation: {e}")
    finally:
        conn.close()


def find_longest_trip_day_postgres():
    """Find the pickup day with the longest trip distance using SQL."""
    query = """
    SELECT
        DATE(lpep_pickup_datetime) AS pickup_date,
        MAX(trip_distance) AS longest_trip_distance
    FROM green_tripdata
    WHERE lpep_pickup_datetime >= '2019-10-01'
      AND lpep_pickup_datetime < '2019-11-01'
    GROUP BY pickup_date
    ORDER BY longest_trip_distance DESC
    LIMIT 1;
    """
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return {"Pickup Date": result[0], "Longest Trip Distance": result[1]}
    except Exception as e:
        print(f"Error finding longest trip day: {e}")
    finally:
        conn.close()


def find_top_pickup_zones_postgres():
    """Find the top 3 pickup zones with total_amount > 13,000 on a specific date."""
    query = """
    SELECT
        tz.Zone,
        SUM(gt.total_amount) AS total_amount
    FROM green_tripdata gt
    JOIN taxi_zone_lookup tz ON gt.PULocationID = tz.LocationID
    WHERE DATE(gt.lpep_pickup_datetime) = '2019-10-18'
    GROUP BY tz.Zone
    HAVING SUM(gt.total_amount) > 13000
    ORDER BY total_amount DESC
    LIMIT 3;
    """
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
        return [{"Zone": row[0], "Total Amount": row[1]} for row in result]
    except Exception as e:
        print(f"Error finding top pickup zones: {e}")
    finally:
        conn.close()


def find_largest_tip_postgres():
    """Find the dropoff zone with the largest tip for pickups in East Harlem North."""
    query = """
    SELECT
        tz.Zone,
        MAX(gt.tip_amount) AS largest_tip
    FROM green_tripdata gt
    JOIN taxi_zone_lookup tz ON gt.DOLocationID = tz.LocationID
    WHERE gt.PULocationID = (
        SELECT LocationID FROM taxi_zone_lookup WHERE Zone = 'East Harlem North'
    )
    GROUP BY tz.Zone
    ORDER BY largest_tip DESC
    LIMIT 1;
    """
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return {"Dropoff Zone": result[0], "Largest Tip": result[1]}
    except Exception as e:
        print(f"Error finding largest tip: {e}")
    finally:
        conn.close()


# Main Execution
if __name__ == "__main__":
    trip_data_path = '../data/green_tripdata_2019-10.csv'
    zone_lookup_path = '../data/taxi_zone_lookup.csv'

    # Create the database if it doesn't exist
    create_database_if_not_exists()

    # Load data into PostgreSQL
    load_data_to_postgres(trip_data_path, zone_lookup_path)

    # Question 3: Trip Segmentation Count
    segmentation_results = calculate_trip_segmentation_postgres()
    print("## 📊 Question 3: Trip Segmentation Count")
    for key, value in segmentation_results.items():
        print(f"{key}: {value}")

    # Question 4: Longest Trip for Each Day
    longest_trip_day = find_longest_trip_day_postgres()
    print("\n## 🕒 Question 4: Longest Trip")
    print(f"Pickup Date: {longest_trip_day['Pickup Date']}")
    print(f"Longest Trip Distance: {longest_trip_day['Longest Trip Distance']}")

    # Question 5: Three Biggest Pickup Zones
    top_pickup_zones = find_top_pickup_zones_postgres()
    print("\n## 🌆 Question 5: Biggest Pickup Zones")
    for zone in top_pickup_zones:
        print(f"Zone: {zone['Zone']}, Total Amount: {zone['Total Amount']}")

    # Question 6: Largest Tip
    largest_tip = find_largest_tip_postgres()
    print("\n## 💰 Question 6: Largest Tip")
    print(f"Dropoff Zone: {largest_tip['Dropoff Zone']}, Largest Tip: {largest_tip['Largest Tip']}")
