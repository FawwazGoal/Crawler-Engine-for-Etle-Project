from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime
from datetime import datetime
import schedule
import time

try:
    # Initialize database connection
    engine = create_engine('postgresql+pg8000://postgres:Fawwaz410133@localhost/etle_violations')

    # Define table schema
    metadata = MetaData()
    violations_table = Table('violations', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('location', String),
        Column('penalty_type_id', String),
        Column('penalty_type_en', String),
        Column('status', Integer),
        Column('capture_date', DateTime)
    )

    # Create the table
    metadata.create_all(engine)

    # Sample payload from ETLE system
    sample_payload = {
        "location": "Jalan Veteran Kota Serang",
        "penaltyTypeId": "Tidak menggunakan sabuk pengaman",
        "penaltyTypeEn": "Safety Belt Not Fastened",
        "status": 5,
        "captureDate": 1632904825000
    }

    # Convert Unix timestamp to Python datetime object
    capture_date_dt = datetime.fromtimestamp(sample_payload["captureDate"] / 1000.0)

    # Insert sample payload into the database
    conn = engine.connect()
    trans = conn.begin()  # Begin a transaction
    try:
        conn.execute(violations_table.insert().values(
            location=sample_payload["location"],
            penalty_type_id=sample_payload["penaltyTypeId"],
            penalty_type_en=sample_payload["penaltyTypeEn"],
            status=sample_payload["status"],
            capture_date=capture_date_dt
        ))
        trans.commit()  # Commit the transaction
    except:
        trans.rollback()  # Rollback the transaction in case of error

    conn.close()  # Close the connection

    print("Sample payload inserted into the database.")

except Exception as e:
    print(f"An error occurred: {e}")
    
# Schedule the job function to run every 12 hours
schedule.every(12).hours.do(job)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)

