# Scaletech Technical Assessment - August 2025, Benjamin Lee

## Objective: 
Build a data pipeline to collect and prepare data about data engineering
technologies for downstream analytics teams.

## Requirements:
* Collect data on 10 data engineering technologies listed below from 2 sources:
* GitHub API (repository metrics)
* PyPI API (Python package downloads)
* Technologies: Apache Airflow, dbt, Apache Spark, Pandas, SQLAlchemy, Great
Expectations, Prefect, Apache Kafka, Snowflake-connector-python, DuckDB
* Processing: Prepare clean, validated data ready for analytics consumption
* Storage: Snowflake - use link to open trial account
* Code: Create public repository

## Description:
Code was written up quickly, with certain considerations being ignored due to the
toy nature of the task. 

The code looks at all libraries/technologies described in config.json and looks
into the Github API as well as the PyPI Bigquery API to obtain various stats and 
metrics. These metrics are the ones that are easy to get - i.e. minimal further 
processing. 

This data is loaded onto a staging schema, which is then combined and presented
in the public schema as a datamart. The focus was more on consistency between
ETL processes rather than efficiency/speed.

Staging loads are additive (no truncation occurs) to allow theoretical snapshotting. 
Risks are of data blowout although depends on how often the run is scheduled. 

Datamart only takes the latest input values.

If done fully properly, would be useful to assign proper IDs rather than using the
name given. 

While metrics were arbitrarily chosen without consideration, in a real life 
scenario, we would be asking analysts as to the purpose of the data needed in 
order to create a proper structure that's suitable for the organisation. 

## Other Technical Things

### Env Entries Needed
Obviously the empty bits are to be filled out

#### GitHub API Configuration
GITHUB_TOKEN

#### Google BigQuery Configuration for PyPI
GOOGLE_CLOUD_PROJECT
GOOGLE_APPLICATION_CREDENTIALS

BIGQUERY_LOCATION=US
BIGQUERY_DATASET_ID=bigquery-public-data.pypi

#### Snowflake Config
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA