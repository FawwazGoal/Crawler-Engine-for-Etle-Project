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

# Function to check for duplicate entries
def check_duplicate(connection, data, payload):
    cursor = connection.cursor()
    check_query = sql.SQL(
        "SELECT * FROM violations WHERE location = %s AND penalty_type_id = %s AND penalty_type_en = %s AND status = %s AND plate = %s AND machine_number = %s AND skeleton_number = %s"
    )
    cursor.execute(check_query, (
        data["location"],
        data["penaltyTypeId"],
        data["penaltyTypeEn"],
        data["status"],
        payload["plate"],
        payload["machine-number"],
        payload["skeleton-number"],
    ))
    return cursor.fetchone()

# Function to insert data into PostgreSQL database
def insert_into_db(connection, data, payload):
    cursor = connection.cursor()
    if not check_duplicate(connection, data, payload):  # Insert only if duplicate doesn't exist
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
    plate = input("Enter the plate number: ")
    machine_number = input("Enter the machine number: ")
    skeleton_number = input("Enter the skeleton number: ")
    payload = {"plate": plate, "machine-number": machine_number, "skeleton-number": skeleton_number}

    api_endpoint = "https://belik.etle-korlantas.info/p/violation-vehicles/check-data"
    etle_data = fetch_etle_data(api_endpoint, payload)

    if etle_data:
        violation_data = etle_data["data"][0]
        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            dbname="etle_violations",
            user="postgres",
            password="Fawwaz410133"
        )
        insert_into_db(connection, violation_data, payload)
        connection.close()
    else:
        print("Failed to fetch data from ETLE system.")
    
    time.sleep(12 * 60 * 60)
