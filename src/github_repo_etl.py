import requests
import json
import snowflake.connector
from datetime import datetime
import os
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
        "github_token": os.getenv('GITHUB_TOKEN'),
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
    if not config['github_token']:
        print("Error: GITHUB_TOKEN not found in environment variables")
        exit(1)
    
    if not all(config['snowflake'].values()):
        print("Error: Missing Snowflake environment variables. Required:")
        print("SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
        exit(1)
    
    return config

def get_repo_metrics(library_name, owner, repo, github_token):
    
    # Should probably be refactored given constant call and response

    headers = {'Authorization': f'token {github_token}'}
    url = f"https://api.github.com/repos/{owner}/{repo}"
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching {owner}/{repo}: {response.status_code}")
        return None
    
    data = response.json()
    
    # Basic metrics get initialised
    metrics = {
        'library_name': library_name,
        'owner': owner,
        'repo_name': repo,
        'stars': data.get('stargazers_count', 0),
        'forks': data.get('forks_count', 0),
        'watchers': data.get('watchers_count', 0),
        'open_issues': data.get('open_issues_count', 0),
        'language': data.get('language'),
        'size_kb': data.get('size', 0),
        'created_at': data.get('created_at'),
        'updated_at': data.get('updated_at'),
        'collected_at': datetime.now().isoformat()
    }

    # Contributor Stats
    print(f"Fetching contributor stats...")
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/contributors"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"    Error fetching contributor stats: {response.status_code}")
        metrics['total_contributors'] = None
        metrics['total_commits'] = None
    else: 
        contributors = response.json()
        metrics['total_contributors'] = len(contributors)
        metrics['total_commits'] = sum(contrib.get('total', 0) for contrib in contributors)

    # Commit Activity Stats
    print(f"Fetching granular commit activity stats...")
    url = f"https://api.github.com/repos/{owner}/{repo}/stats/commit_activity"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"    Error fetching commit activity stats: {response.status_code}")
        metrics['commits_last_year'] = None
        metrics['commits_last_month'] = None
    else: 
        commit_activity = response.json()
        metrics['commits_last_year'] = sum(week.get('total', 0) for week in commit_activity)
        # Get recent activity (last 4 weeks)
        recent_weeks = commit_activity[-4:] if len(commit_activity) >= 4 else commit_activity
        metrics['commits_last_month'] = sum(week.get('total', 0) for week in recent_weeks)

    # Finished!
    print(f"{owner}/{repo} finished!")
    return metrics

def insert_metrics(metrics_list, snowflake_config):
    # Inserts metrics into the pre-built Snowflake DB

    if not metrics_list:
        print("No metrics to insert")
        return

    conn = snowflake.connector.connect(**snowflake_config)
    cursor = conn.cursor()

    sql = """
    INSERT INTO github_repo_metrics 
    (name, github_owner, github_repository_name, stars, forks, watchers, 
        open_issues, language, size_kb, created_at, updated_at, collected_at, 
        total_contributors, total_commits, commits_last_year, commits_last_month)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for metrics in metrics_list:
        cursor.execute(sql, (
            metrics['library_name'],
            metrics['owner'],
            metrics['repo_name'],
            metrics['stars'],
            metrics['forks'],
            metrics['watchers'],
            metrics['open_issues'],
            metrics['language'],
            metrics['size_kb'],
            metrics['created_at'],
            metrics['updated_at'],
            metrics['collected_at'],
            metrics['total_contributors'],
            metrics['total_commits'],
            metrics['commits_last_year'],
            metrics['commits_last_month']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(metrics_list)} records into Snowflake")

def main():
    # Load configuration
    config = load_config()
    
    # Collect metrics for all libraries
    all_metrics = []


    for library in config['libraries']:
        print(f"Fetching metrics for {library['name']} ({library['github_owner']}/{library['github_repo']})...")

        metrics = get_repo_metrics(
            library['name'],
            library['github_owner'], 
            library['github_repo'],
            config['github_token']
        )

        if metrics:
            all_metrics.append(metrics)

    insert_metrics(all_metrics, config['snowflake'])

    print('ETL for Github Complete!')

if __name__ == "__main__":
    main()