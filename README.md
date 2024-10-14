# Snowflake to Jetsons Data Transfer

This project facilitates the transfer of data from Snowflake to a SQL Server database (Jetsons). It handles covered entities and their parent-child relationships.

## Recent Enhancements

1. **Environment-specific configurations**: The script now supports separate configurations for test and production environments using `.env.test` and `.env.production` files.

2. **Duplicate record handling**: The `insert_ce_parents` function now checks for existing records before insertion, preventing duplicate entries in the `ceparentchild` table.

3. **Batch processing**: Large datasets are now processed in batches to improve performance and reduce memory usage.

4. **Improved logging**: The script now uses Python's logging module for better tracking of the data transfer process.

## Prerequisites

- Python 3.7+
- Required Python packages (see `requirements.txt`)
- Access to Snowflake and SQL Server databases
- ODBC Driver 17 for SQL Server

## Setup

1. Clone this repository.
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Set up your environment files:
   - Create `.env.test` for the test environment
   - Create `.env.production` for the production environment

   Both files should contain the following variables:
   ```
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
   SNOWFLAKE_DATABASE=your_snowflake_database
   SNOWFLAKE_ROLE=your_snowflake_role
   OKTA_USERNAME=your_okta_username
   OKTA_PASSWORD=your_okta_password
   KWEB_SERVER=your_kweb_server
   KWEB_DATABASE=your_kweb_database
   JETSONS_USERNAME=your_jetsons_username
   JETSONS_PASSWORD=your_jetsons_password
   JETSONS_USER_ID=your_jetsons_user_id
   ```

## Usage

Run the script with the following command:
The test env is selected by default.
```
python main.py --env {test|production}
```

