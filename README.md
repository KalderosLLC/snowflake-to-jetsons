# Snowflake to Jetsons

This repository contains code for migrating data from Snowflake to Jetsons.

## Overview

The Snowflake to Jetsons project is designed to facilitate the transfer of data from a Snowflake database to a Jetsons system. This migration tool aims to streamline the process of moving data between these two platforms, ensuring data integrity and efficiency.

## Features

- Extracts data from Snowflake tables
- Transforms data to fit Jetsons schema
- Loads data into Jetsons system
- Supports incremental data updates
- Provides error handling and logging

## Getting Started

### Prerequisites

- Python 3.7+
- Snowflake account and credentials
- Jetsons account and API access

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/snowflake-to-jetsons.git
   ```

2. Install required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure your Snowflake and Jetsons credentials in the `config.yaml` file.

### Usage

Run the main script to start the migration:
```
python main.py
```

### Configuration

The `.env` file contains the following configurations:

- `SNOWFLAKE_ACCOUNT`: Snowflake account
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse
- `SNOWFLAKE_DATABASE`: Snowflake database
- `SNOWFLAKE_ROLE`: Snowflake role
- `JETSONS_USERNAME`: Jetsons username
- `JETSONS_PASSWORD`: Jetsons password
