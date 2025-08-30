import pandas as pd
from google.cloud import bigquery
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os
import json
from dotenv import load_dotenv

def load_config():
    """Load configuration from config.json file and environment variables"""
    # Load environment variables
    load_dotenv()
    
    # Load libraries from config file
    try:
        with open('config.json', 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found")
        exit(1)
    
    # Get credentials from environment variables
    config = {
        "gcp_project": os.getenv('GOOGLE_CLOUD_PROJECT'),
        "gcp_credentials": os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        "libraries": config_data.get('libraries', []),
        "snowflake": {
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "account": os.getenv('SNOWFLAKE_ACCOUNT'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "database": os.getenv('SNOWFLAKE_DATABASE'),
            "schema": os.getenv('SNOWFLAKE_SCHEMA')
        }
    }
    
    # Validate required environment variables
    if not config['gcp_project']:
        print("Error: GOOGLE_CLOUD_PROJECT not found in environment variables")
        exit(1)

    if not config['gcp_credentials']:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")
        exit(1)
    
    if not all(config['snowflake'].values()):
        print("Error: Missing Snowflake environment variables. Required:")
        print("SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
        exit(1)
    
    return config

def get_pypi_stats(libraries, gcp_project):
    client = bigquery.Client(project=gcp_project)

    # Create the package list for SQL IN clause
    package_list = "', '".join([lib['pypi_package'] for lib in libraries])
    
    query = f"""
    SELECT
        file.project as pypi_name,
        COUNT(*) as total_downloads_alltime,
        -- note four weeks are used so that it matches with the Github
        COUNTIF(DATE(timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK) AND CURRENT_DATE()) AS downloads_last_month,
        COUNTIF(DATE(timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND CURRENT_DATE()) AS downloads_last_year
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE file.project IN ('{package_list}')
    GROUP BY 1
    ORDER BY pypi_name, total_downloads_alltime DESC
    """
    
    print("Fetching PyPI download stats...")
    df = client.query(query).to_dataframe()
    print(f"Retrieved {len(df)} rows")

    # Clean up attaching metadata (name and collected timestamp) to the df
    package_to_name = {lib['pypi_package']: lib['name'] for lib in libraries}
    
    # Add library_name column by mapping package_name
    df['name'] = df['pypi_name'].map(package_to_name)

    return df

def load_to_snowflake(df, snowflake_config):
    # inserts stats into pre-built snowflake DB
    
    if df.empty:
        print("No metrics to insert")
        return

    conn = snowflake.connector.connect(**snowflake_config)
    cursor = conn.cursor()

    
    sql = """
    INSERT INTO pypi_download_stats 
    (name, pypi_name, total_downloads_alltime, 
     downloads_last_month, downloads_last_year)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    # highly inefficient; will need to use proper write_pandas for proper stuff
    # kept it this way as it's similar to the github load for consistency 
    for _, row in df.iterrows():

        cursor.execute(sql, (
            row['name'],
            row['pypi_name'],
            int(row['total_downloads_alltime']),
            int(row['downloads_last_month']),
            int(row['downloads_last_year'])
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(df)} records into Snowflake")


def main():
    # Load configuration
    config = load_config()
    
    download_stats = get_pypi_stats(
        config['libraries'], 
        config['gcp_project'])

    load_to_snowflake(download_stats, config['snowflake'])

if __name__ == "__main__":
    main()