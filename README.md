# Stock Market ETL Pipeline

This project performs ETL (Extract, Transform, Load) operations on stock market data using Airflow and Google Cloud.

## Setup

### 1. Create a `.env` File

- Place the `.env` file anywhere in the directory (e.g., in the `etl` folder).

#### Example `.env` file:

```bash
AIRFLOW_UID=50000
GCP_PROJECT_ID=etl-melon-00010
BQ_DATASET_ID=stock_study_prices
ALPHA_API_KEY=FJHSDF798F
GCP_SERVICE_JSON_LOC=config/etl-melon-00010-h4j23hjk.json
```

### 2. Create a conneciton in Airflow to Google Cloud
![airflow google cloud connection settings](https://github.com/ChiaYunhan/etl_gcp/blob/main/airflow%20googel%20cloud%20connection.png?raw=true)

### 3. Simple run `docker compose up (optionally with -d)`

# Current Issues (trying to fix or solve)
1. **Table schema with multiple symbols**:
    - When using more than one symbol (e.g., ['AAPL', 'GOOGL', 'MSFT']), only the last symbol (MSFT) generates the table with the correct schema. Previous symbols create schemaless tables, causing issues in the backfill workflow.

# Future Development 
1. **Simple holiday and weekend checks:**
    - Add tasks to check if the current day is a holiday or weeked, if so, the pipeline won't run.
2. **Adding more data sources:**
    -Incorporate news sentiment data from:
        -**Alpha Vantage** (provides sentiment directly)
        -**News AP** (requires performing sentiment analysis indepedently)
    -Merge insights from stock data and news sentiment for better analysis.
