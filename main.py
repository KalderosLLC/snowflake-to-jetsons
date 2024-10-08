import logging
import os
import pyodbc
from dotenv import load_dotenv
from snowflake.connector import connect as snowflake_connect
from sqlalchemy import create_engine
from snowflake_connection import SnowflakeDatasource
from jetson_connection import JetsonDatasource

# Set the logging level
logging.basicConfig(level=logging.INFO)

# Determine the environment (default to 'development' if not set)
env = os.getenv('APP_ENV', 'test')

# Load the correct .env file based on the environment
dotenv_file = f'.env.{env}'
load_dotenv(dotenv_file)

# Load environment variables from .env
load_dotenv()
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_role = os.getenv('SNOWFLAKE_ROLE')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
okta_username = os.getenv('OKTA_USERNAME')
snowflake_password = os.getenv('OKTA_PASSWORD')
kweb_server = os.getenv('KWEB_SERVER')
kweb_database = os.getenv('KWEB_DATABASE')
jetson_username = os.getenv('JETSONS_USERNAME')
jetson_password = os.getenv('JETSONS_PASSWORD')
jetson_user_id = os.getenv('JETSONS_USER_ID')

# DB connections
## Connect to Snowflake
snowflake_conn = snowflake_connect(
    account=snowflake_account,
    user=okta_username,
    password=snowflake_password,
    warehouse=snowflake_warehouse,
    role=snowflake_role,
    database=snowflake_database
)

## Connect to Jetsons
# Create SQL engine
sql_server_engine = create_engine(f"mssql+pyodbc://{jetson_username}:{jetson_password}@{kweb_server}/{kweb_database}?driver=ODBC+Driver+17+for+SQL+Server")

# Create a SnowflakeDataSource object
snowflake_ds = SnowflakeDatasource(snowflake_conn, snowflake_database)

# Create a JetsonDatasource object
jetson_ds = JetsonDatasource(sql_server_engine, jetson_user_id)

def main():
    # Get and clean covered entities
    covered_entities_df = snowflake_ds.get_covered_entities()

    # Get and clean covered entity identifiers
    # covered_entity_identifiers_df = snowflake_ds.get_covered_entity_identifiers()

    # Insert covered entities
    jetson_ds.insert_covered_entities(covered_entities_df)

    # Insert covered entity identifiers
    # jetson_ds.insert_covered_entity_identifiers(covered_entity_identifiers_df)

    # Get and clean ce parents
    ce_parents_df = snowflake_ds.get_ce_parents()

    # Insert ce parents
    jetson_ds.insert_ce_parents(ce_parents_df)

if __name__ == "__main__":
    main()




