import json
import requests
import psycopg2
import time
from datetime import datetime
from psycopg2 import sql

# Function to fetch data from ETLE system
def fetch_etle_data(api_endpoint, payload):
    response = requests.get(api_endpoint, params=payload)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None

# Function to insert data into PostgreSQL database
def insert_into_db(connection, data, payload):
    cursor = connection.cursor()
    insert_query = sql.SQL(
        "INSERT INTO violations (location, penalty_type_id, penalty_type_en, status, capture_date, plate, machine_number, skeleton_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    capture_date_dt = datetime.fromtimestamp(data["captureDate"] / 1000.0)
    cursor.execute(insert_query, (
        data["location"],
        data["penaltyTypeId"],
        data["penaltyTypeEn"],
        data["status"],
        capture_date_dt,
        payload["plate"],
        payload["machine-number"],
        payload["skeleton-number"],
    ))
    connection.commit()

while True:
    # Define API endpoint and payload
    api_endpoint = "https://belik.etle-korlantas.info/p/violation-vehicles/check-data"
    payload = {"plate": "A1492RH", "machine-number": "4A91GD9541", "skeleton-number": "MK2NCWHANJJ018193"}

    # Fetch data from ETLE system
    etle_data = fetch_etle_data(api_endpoint, payload)
    if etle_data:
        # Assume the first entry in the "data" list is the one you want
        violation_data = etle_data["data"][0]

        # Connect to PostgreSQL database
        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            dbname="etle_violations",
            user="postgres",
            password="Fawwaz410133"
        )

        # Insert data into database
        insert_into_db(connection, violation_data, payload)

        # Close the database connection
        connection.close()
    else:
        print("Failed to fetch data from ETLE system.")
    
    # Sleep for 12 hours before running again
    time.sleep(12 * 60 * 60)
