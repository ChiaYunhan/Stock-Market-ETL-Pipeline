# Stock Market ETL Pipeline

This project performs ETL (Extract, Transform, Load) operations on stock market data from a web API using Airflow and Google Cloud.

## Workflow

![workflow](https://github.com/ChiaYunhan/etl_gcp/blob/main/workflow.png?raw=true)

## Pre req
1. Docker desktop
2. BigQuery billing stuff
3. create GCP project etc...

## Setup

### 1. Create a `.env` File

- Place the `.env` file anywhere in the directory (e.g., in the `etl` folder).

#### Example `.env` file: (not real values, be sure to replace with your own)

```bash
AIRFLOW_UID=50000 #keep at 50000
GCP_PROJECT_ID=etl-melon-00010
BQ_DATASET_ID=stock_study_prices
ALPHA_API_KEY=FJHSDF798F
GCP_SERVICE_JSON_LOC=config/etl-melon-00010-h4j23hjk.json
```

### 2. 
```bash
cd etl
# to start
docker compose up -d
# to stop (optionally, -v), -v removes data in volumes as well, in this case the connection config to BQ you created earlier
docker compose down 
```

### 3. Create a conneciton in Airflow to Google Cloud
![airflow google cloud connection settings](https://github.com/ChiaYunhan/etl_gcp/blob/main/airflow%20googel%20cloud%20connection.png?raw=true)

### 4. Trigger DAG 


# Current Issues (trying to fix)
1. **Table schema with multiple symbols**:
    - When using more than one symbol (e.g., ['AAPL', 'GOOGL', 'MSFT']), only the last symbol (MSFT) generates the table with the correct schema. Previous symbols create schemaless tables, causing issues in the DAG.

# Future Development 
1. **Simple holiday and weekend checks:**
    - Add tasks to check if the current day is a holiday or weeked, if so, the pipeline won't run.
2. **Adding more data sources**:
    - Incorporate news sentiment data from:
        - **Alpha Vantage** (provides sentiment directly).
        - **News API** (requires performing sentiment analysis independently).
    - Merge insights from stock data and news sentiment for better analysis.
