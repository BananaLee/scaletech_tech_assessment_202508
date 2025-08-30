import snowflake.connector
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
    if not all(config['snowflake'].values()):
        print("Error: Missing Snowflake environment variables. Required:")
        print("SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
        exit(1)
    
    return config

def load_to_snowflake(snowflake_config):

    conn = snowflake.connector.connect(**snowflake_config)
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE public.tech_metrics")

        sql = """
        INSERT INTO public.tech_metrics (
            name, github_owner, github_repository_name, pypi_name, language, 
            stars, forks, watchers, open_issues, size_kb,
            created_at, updated_at, total_contributors, total_commits,
            total_downloads_alltime, commits_last_year, downloads_last_year,
            commits_last_month, downloads_last_month
        )

        WITH latest_github AS (
            SELECT gm.*
            FROM github_repo_metrics gm
            INNER JOIN (
                SELECT name, MAX(collected_at) AS max_collected_at
                FROM github_repo_metrics
                GROUP BY name
            ) latest
            ON gm.name = latest.name AND gm.collected_at = latest.max_collected_at
        ),
        latest_pypi AS (
            SELECT ps.*
            FROM pypi_download_stats ps
            INNER JOIN (
                SELECT name, MAX(downloads_last_year) AS max_downloads_last_year
                FROM pypi_download_stats
                GROUP BY name
            ) latest
            ON ps.name = latest.name 
               AND ps.downloads_last_year = latest.max_downloads_last_year
        )
        SELECT
            g.name,
            g.github_owner,
            g.github_repository_name,
            p.pypi_name,
            g.language,
            g.stars,
            g.forks,
            g.watchers,
            g.open_issues,
            g.size_kb,
            g.created_at,
            g.updated_at,
            g.total_contributors,
            g.total_commits,
            p.total_downloads_alltime, 
            g.commits_last_year, 
            p.downloads_last_year,
            g.commits_last_month, 
            p.downloads_last_month
        FROM latest_github g 
        LEFT JOIN latest_pypi p ON g.name = p.name
        ORDER BY g.name;

        """

        # Execute the combined query
        cursor.execute(sql)
        inserted_count = cursor.rowcount
        
        # Commit the transaction
        conn.commit()
        
        print(f"Successfully inserted {inserted_count} records into public.tech_metrics")
    
    except Exception as e:
        print(f"Error in loading data mart: {str(e)}")
        print("Rolling back...")
        conn.rollback()
        print("Rollback complete")
        raise
    finally:
        cursor.close()
        conn.close()


def main():
    # Load configuration
    config = load_config()
    
    load_to_snowflake(config['snowflake'])

    print('Data mart succesfully loaded!')

if __name__ == "__main__":
    main()