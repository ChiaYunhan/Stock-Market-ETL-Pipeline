import pendulum
import requests
import logging

import pandas as pd
from typing import Union, Dict, Optional
from google.cloud import bigquery
from google.oauth2 import service_account

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# Config variables
BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "etl-gcp-435016"
BQ_DATASET = "stock_analysis"
ALPHA_API_KEY = "LGK4OG1SQ1H5C7Q5"
LOCATION = "asia-southeast1"


def _build_api_url(symbol: str, backfill: bool) -> str:
    """Build the Alpha Vantage API URL with the correct parameters."""

    base_url = "https://www.alphavantage.co/query"
    outputsize = "full" if backfill else "compact"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": ALPHA_API_KEY,
        "outputsize": outputsize,
    }

    return base_url, params


def _fetch_price_from_api(base_url: str, params: dict) -> Optional[list[dict]]:
    """Fetch data from the API and handle errors."""
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if "Meta Data" not in data:
            logging.info(f"API Message: {data['Information']}")
            return None

        time_series = data["Time Series (Daily)"]
        return time_series

    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None


def _get_difference_type(difference: float):
    if difference > 0:
        return "positive"
    elif difference < 0:
        return "negative"
    else:
        return "no change"


def _process_backfill_data(stock_prices: list[dict]) -> pd.DataFrame:
    """Convert the time series data to a pandas DataFrame."""
    stock_info = [
        {
            "Date": date,
            "Open": values["1. open"],
            "High": values["2. high"],
            "Low": values["3. low"],
            "Close": values["4. close"],
            "Volume": values["5. volume"],
        }
        for date, values in stock_prices.items()
    ]
    stock_prices = pd.DataFrame(stock_info)
    stock_prices["Date"] = pd.to_datetime(stock_prices["Date"]).dt.date
    stock_prices["Open"] = stock_prices["Open"].astype(float)
    stock_prices["High"] = stock_prices["High"].astype(float)
    stock_prices["Low"] = stock_prices["Low"].astype(float)
    stock_prices["Close"] = stock_prices["Close"].astype(float)
    stock_prices["Volume"] = stock_prices["Volume"].astype(int)
    stock_prices["Difference"] = stock_prices["Open"] - stock_prices["Close"]
    stock_prices["Difference_type"] = stock_prices["Difference"].apply(
        _get_difference_type
    )

    return stock_prices


def _process_latest_data(stock_prices: list[dict]) -> Dict[str, Union[str, float, int]]:
    """Extract the latest stock data from the time series."""
    # pendulum.yesterday() used instead of today() because scheduled to run around 4am myt but around closing time for NASDAQ
    date = pendulum.yesterday().to_date_string()
    latest_entry = stock_prices.get(date, {})

    if latest_entry:
        difference = float(latest_entry.get("2. high")) - float(
            latest_entry.get("4. close")
        )
        difference_type = (
            "positive"
            if difference > 0
            else ("negative" if difference < 0 else "no change")
        )
        return {
            "Date": date,
            "Open": float(latest_entry.get("1. open")),
            "High": float(latest_entry.get("2. high")),
            "Low": float(latest_entry.get("3. low")),
            "Close": float(latest_entry.get("4. close")),
            "Volume": int(latest_entry.get("5. volume")),
            "Difference": difference,
            "Difference_type": difference_type,
        }
    else:
        return {
            "Date": date,
            "Open": None,
            "High": None,
            "Low": None,
            "Close": None,
            "Volume": None,
            "Difference": None,
            "Difference_type": "no_change",
        }


@dag(
    dag_id="stock_etl_gcp",
    schedule="15 16 * * *",
    start_date=pendulum.datetime(2024, 9, 16, tz="America/New_York"),
    end_date=pendulum.datetime(2024, 9, 24, tz="America/New_York"),
    catchup=False,
)
def etl():
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

    @task
    def check_price_table_exists(symbol: str) -> bool:
        return hook.table_exists(
            project_id=BQ_PROJECT, dataset_id=BQ_DATASET, table_id=f"{symbol}_prices"
        )

    @task
    def create_price_table(symbol: str) -> None:
        schema = [
            {"name": "Date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "Open", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "High", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Low", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Close", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Volume", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Difference", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Difference_type", "type": "STRING", "mode": "NULLABLE"},
        ]
        hook.create_empty_table(
            project_id=BQ_PROJECT,
            dataset_id=BQ_DATASET,
            table_id=f"{symbol}_prices",
            schema_fields=schema,
            table_resource={
                "description": f"Table containing the prices of {symbol} since the 1990s"
            },
        )
        print("##############")
        print(f"`{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices` CREATED.")
        print("##############")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def delete_price_table(symbol: str) -> None:
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices"
        hook.delete_table(project_id=BQ_PROJECT, table_id=table_id)

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def upload_backfill_bq(data: pd.DataFrame):
        credentials = service_account.Credentials.from_service_account_file(
            "config/etl-gcp-435016-ba59ebfa83fd.json"
        )
        client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices"
        job = client.load_table_from_dataframe(data, table_id)
        job.result()

    @task
    def get_latest_row_date(symbol: str) -> pendulum.DateTime:
        sql_query = f"""
        SELECT Date
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices`
        ORDER BY Date DESC
        LIMIT 1;  
        """
        job_id = hook.insert_job(
            configuration={"query": {"query": sql_query, "useLegacySql": False}}
        )

        result = hook.get_query_results(
            job_id=job_id,
            location=LOCATION,
            selected_fields="Date",
            project_id=BQ_PROJECT,
        )

        # Extract the date from the result
        latest_date = result[0]["Date"]  # This will be a datetime.date object

        # Convert the date to string (e.g., "2024-09-16")
        latest_date_str = latest_date.strftime("%Y-%m-%d")

        return latest_date_str

    @task()
    def insert_row_into_bq(data: dict, symbol: str):
        """
        Insert a row into BigQuery using the insert_all method and log the data being inserted.

        :param data: A dictionary containing the data for one row.
        :param symbol: The stock symbol used to identify the correct BigQuery table.
        """
        row = [data]

        # Log the data being inserted
        logging.info(f"Inserting data into {symbol}_prices: {data}")

        hook.insert_all(
            project_id=BQ_PROJECT,
            dataset_id=BQ_DATASET,
            table_id=f"{symbol}_prices",
            rows=row,
            fail_on_error=True,
        )

    for symbol in ["AAPL", "MSFT", "GOOGL", "NFLX"]:
        check_price_table = check_price_table_exists.override(
            task_id=f"check_{symbol}_price_table"
        )(symbol)

        join_1 = EmptyOperator(
            task_id=f"{symbol}_end_task",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        @task.branch()
        def branch_1(symbol=symbol, ti=None) -> str:
            price_table_exists = ti.xcom_pull(task_ids=f"check_{symbol}_price_table")

            if price_table_exists:
                return f"get_latest_entry_{symbol}_price"
            else:
                return f"create_backfill_{symbol}"

        b1 = branch_1.override(task_id=f"branch_1_{symbol}")()

        check_price_table >> b1

        @task
        def fetch_stock_data(
            symbol: str, backfill: bool = False
        ) -> Union[Dict[str, Union[str, float, int]], pd.DataFrame]:
            """
            Fetch latest stock data for a given symbol from Alpha Vantage API.
            If backfill is True, returns historical data as a pandas DataFrame.
            Otherwise, returns the latest available stock data as a dictionary.
            """
            base_url, params = _build_api_url(symbol, backfill=backfill)

            # Fetch data from the API
            data = _fetch_price_from_api(base_url, params)
            if not data:
                # Return default empty results for failed API calls
                return {
                    "Date": pendulum.yesterday().to_date_string(),
                    "Open": None,
                    "High": None,
                    "Low": None,
                    "Close": None,
                    "Volume": None,
                    "Difference": None,
                    "Difference_type": None,
                }

            # Process the time series data
            if backfill:
                return _process_backfill_data(data)
            else:
                return _process_latest_data(data)

        with TaskGroup(group_id=f"create_backfill_{symbol}") as create_backfill:
            create_table = create_price_table.override(
                task_id=f"inner_create_{symbol}_prices_table"
            )(symbol=symbol)
            data = fetch_stock_data.override(task_id=f"inner_fetch_{symbol}_prices")(
                symbol, backfill=True
            )
            upload = upload_backfill_bq.override(
                task_id=f"upload_{symbol}_backfill_bq"
            )(data)
            delete = delete_price_table.override(
                task_id=f"delete_{symbol}_prices_table"
            )(symbol)

            [create_table, data] >> upload
            [create_table, data] >> delete

        latest_entry = get_latest_row_date.override(
            task_id=f"get_latest_entry_{symbol}_price"
        )(symbol)

        b1 >> Label("Table doesnt exists") >> create_backfill >> join_1
        b1 >> Label("Table exists") >> latest_entry

        @task.branch()
        def branch_2(symbol=symbol, ti=None) -> str:
            latest_date = ti.xcom_pull(task_ids=f"get_latest_entry_{symbol}_price")

            if latest_date == pendulum.yesterday().to_date_string():
                return f"do_nothing_{symbol}"

            else:
                return f"fetch_insert_{symbol}"

        b2 = branch_2.override(task_id=f"branch_2_{symbol}")()

        latest_entry >> b2

        with TaskGroup(group_id=f"fetch_insert_{symbol}") as fetch_insert:
            data = fetch_stock_data(symbol)

            insert_job = insert_row_into_bq(data, symbol)

            data >> insert_job

        do_nothing = EmptyOperator(task_id=f"do_nothing_{symbol}")

        join_2 = EmptyOperator(
            task_id=f"join_2_{symbol}",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        (
            b2
            >> Label(f"{symbol}_prices not up to date")
            >> fetch_insert
            >> join_2
            >> join_1
        )
        b2 >> Label(f"{symbol}_prices up to date") >> do_nothing >> join_2 >> join_1


etl()
