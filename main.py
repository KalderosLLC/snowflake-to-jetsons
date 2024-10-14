import logging
import os
import argparse
from dotenv import load_dotenv
from snowflake.connector import connect as snowflake_connect
from sqlalchemy import create_engine
from snowflake_connection import SnowflakeDatasource
from jetson_connection import JetsonDatasource

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables(env):
    env_file = f".env.{env}"
    if not os.path.exists(env_file):
        raise FileNotFoundError(f"Environment file not found: {env_file}")
    
    load_dotenv(env_file)
    logger.info(f"Loaded environment variables from {env_file}")
    
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_ROLE',
        'OKTA_USERNAME', 'OKTA_PASSWORD', 'KWEB_SERVER', 'KWEB_DATABASE',
        'JETSONS_USERNAME', 'JETSONS_PASSWORD', 'JETSONS_USER_ID'
    ]
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"Missing required environment variable: {var}")
    logger.info("All required environment variables are present")

def create_snowflake_engine():
    try:
        snowflake_conn = snowflake_connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('OKTA_USERNAME'),
            password=os.getenv('OKTA_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Snowflake connection created successfully")
        return snowflake_conn
    except Exception as e:
        logger.error(f"Failed to create Snowflake connection: {str(e)}")
        raise

def create_sql_server_engine():
    try:
        connection_string = (
            f"mssql+pyodbc://{os.getenv('JETSONS_USERNAME')}:{os.getenv('JETSONS_PASSWORD')}@"
            f"{os.getenv('KWEB_SERVER')}/{os.getenv('KWEB_DATABASE')}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        sql_server_engine = create_engine(connection_string)
        logger.info("SQL Server engine created successfully")
        return sql_server_engine
    except Exception as e:
        logger.error(f"Failed to create SQL Server engine: {str(e)}")
        raise

def main(env):
    try:
        load_environment_variables(env)
        
        snowflake_conn = create_snowflake_engine()
        sql_server_engine = create_sql_server_engine()

        snowflake_datasource = SnowflakeDatasource(snowflake_conn, os.getenv('SNOWFLAKE_DATABASE'))
        jetson_datasource = JetsonDatasource(sql_server_engine, os.getenv('JETSONS_USER_ID'))

        logger.info(f"Starting data transfer process in {env} environment")

        # Covered Entities
        covered_entities_df = snowflake_datasource.get_covered_entities()
        logger.info(f"Retrieved {len(covered_entities_df)} covered entities from Snowflake")
        result = jetson_datasource.insert_covered_entities(covered_entities_df)
        if result is not None:
            logger.info("Covered entities inserted successfully")
        else:
            logger.warning("No covered entities were inserted")

        # CE Parents
        ce_parents_df = snowflake_datasource.get_ce_parents()
        logger.info(f"Retrieved {len(ce_parents_df)} CE parents from Snowflake")
        result = jetson_datasource.insert_ce_parents(ce_parents_df)
        if result is not None:
            logger.info("CE parents inserted successfully")
        else:
            logger.warning("No CE parents were inserted")

        logger.info("Data transfer process completed successfully")

    except Exception as e:
        logger.error(f"An error occurred during the data transfer process: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data transfer process with specified environment.")
    parser.add_argument('--env', choices=['test', 'production'], default='test',
                        help="Specify the environment to use (test or production)")
    args = parser.parse_args()
    
    main(args.env)
