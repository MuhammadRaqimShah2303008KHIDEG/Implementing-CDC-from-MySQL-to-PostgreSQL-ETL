from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes
from pyflink.common import Row
import psycopg2
from psycopg2.extras import execute_batch
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

# Get the API key
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user = os.getenv("USER")
password = os.getenv("PASSWORD")


mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")


# PostgreSQL connection details
PG_CONFIG = {
    'host': host,
    'port': port,
    'database': database,
    'user': user,
    'password': password
}

# Setup PyFlink environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=env_settings)

# Source: MySQL CDC
t_env.execute_sql("""
    CREATE TABLE mysql_source (
        `transactionID` INT,
        `ACR` STRING,
        `amount` DECIMAL(11,2),
        PRIMARY KEY (`transactionID`) NOT ENFORCED
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = {mysql_host},
        'port' = {mysql_port},
        'username' = {mysql_user},
        'password' = {mysql_password},
        'database-name' = {mysql_database},
        'table-name' = 'demo',
        'server-time-zone' = 'Asia/Karachi',
        'scan.startup.mode' = 'latest-offset'
    )
""")

print("\n🧾 MySQL Source Table Schema:")
mysql_desc = t_env.execute_sql("DESCRIBE mysql_source")
print(mysql_desc)

# Create a temporary Flink table to collect data (using print connector for debugging)
t_env.execute_sql("""
    CREATE TABLE temp_sink (
        `transactionID` INT,
        `ACR` STRING,
        `amount` DECIMAL(11,2),
        PRIMARY KEY (`transactionID`) NOT ENFORCED
    ) WITH (
        'connector' = 'print'
    )
""")


# Custom Python function to write to PostgreSQL with proper quoting
def write_to_postgres_batch(rows):
    """
    Write rows to PostgreSQL with properly quoted case-sensitive column names
    """
    conn = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # SQL with properly quoted column names for PostgreSQL case-sensitivity
        upsert_sql = """
            INSERT INTO "dev_practice_db"."demo" ("dbId", "transactionID", "ACR", "amount")
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("transactionID") 
            DO UPDATE SET 
                "dbId" = EXCLUDED."dbId",
                "ACR" = EXCLUDED."ACR",
                "amount" = EXCLUDED."amount"
        """
        
        # Prepare batch data
        batch_data = []
        for row in rows:
            batch_data.append((
                3,  # dbId
                row['transactionID'],
                row['ACR'],
                float(row['amount']) if row['amount'] else None
            ))
        
        # Execute batch
        execute_batch(cursor, upsert_sql, batch_data, page_size=100)
        conn.commit()
        
        logger.info(f"✅ Successfully wrote {len(batch_data)} rows to PostgreSQL")
        
    except Exception as e:
        logger.error(f"❌ Error writing to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


# Option 1: Use Flink's forEach to process each row
print("\n✅ Starting CDC pipeline with custom PostgreSQL writer...")
print("📊 Monitoring MySQL changes and writing to PostgreSQL with quoted column names...\n")

# Get the table as a result
table_result = t_env.execute_sql("""
    SELECT 
        `transactionID`,
        `ACR`,
        `amount`
    FROM mysql_source
""")

# Collect and process rows in batches
batch = []
BATCH_SIZE = 3

try:
    with table_result.collect() as results:
        for row in results:
            # Convert Row to dict
            row_dict = {
                'transactionID': row[0],
                'ACR': row[1],
                'amount': row[2]
            }
            
            batch.append(row_dict)
            
            # Write batch when size is reached
            if len(batch) >= BATCH_SIZE:
                write_to_postgres_batch(batch)
                batch = []
        
        # Write remaining rows
        if batch:
            write_to_postgres_batch(batch)
            
except KeyboardInterrupt:
    logger.info("\n⏹️  Pipeline stopped by user")
    if batch:
        write_to_postgres_batch(batch)
except Exception as e:
    logger.error(f"❌ Pipeline error: {e}")
    raise