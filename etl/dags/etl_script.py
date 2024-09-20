import pendulum
import requests
import logging
import os

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
from google.api_core import exceptions


# Config variables
BQ_CONN_ID = "my_gcp_conn"
LOCATION = "asia-southeast1"
BQ_PROJECT = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET_ID")
ALPHA_API_KEY = os.environ.get("APLHA_API_KEY")
GCP_SERVICE_JSON_LOC = os.environ.get("GCP_SERVICE_JSON_LOC")


def _build_alpha_api_url(symbol: str, backfill: bool) -> str:
    """
    Build the Alpha Vantage API URL with the correct parameters for fetching stock data.

    Args:
        symbol (str): Stock symbol to fetch the data for.
        backfill (bool): Whether to fetch full data ('full') or recent data ('compact').

    Returns:
        str: The full API URL with parameters for Alpha Vantage.
    """

    base_url = "https://www.alphavantage.co/query"
    outputsize = "full" if backfill else "compact"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": ALPHA_API_KEY,
        "outputsize": outputsize,
    }

    return base_url, params


def _fetch_price_from_api(base_url: str, params: dict) -> Union[bool, list[dict]]:
    """
    Fetch stock prices from the Alpha Vantage API and handle any errors.

    Args:
        base_url (str): The base API URL.
        params (dict): The parameters to be passed to the API.

    Returns:
        Optional[list[dict]]: Time series stock data as a dictionary or None if an error occurs.
    """
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Check if the API returned valid data
        if "Information" in data:
            logging.info(f"API Message: {data['Information']}")
            return False

        time_series = data["Time Series (Daily)"]
        return time_series

    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return False


def _get_difference_type(difference: float) -> str:
    """
    Determine the type of difference (positive, negative, or no change) based on the stock price difference.

    Args:
        difference (float): The difference between opening and closing prices.

    Returns:
        str: 'positive', 'negative', or 'no change' based on the value of the difference.
    """
    if difference > 0:
        return "positive"
    elif difference < 0:
        return "negative"
    else:
        return "no change"


def _process_backfill_data(stock_prices: dict) -> pd.DataFrame:
    """
    Process historical stock data for backfill and convert it into a pandas DataFrame.

    Args:
        stock_prices (list[dict]): Historical stock price data fetched from the API.

    Returns:
        pd.DataFrame: Processed DataFrame containing stock price information for multiple dates.
    """
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

    # Convert the list of dictionaries into a DataFrame and process the data
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
    """
    Process the most recent stock price data and return it as a dictionary.

    Args:
        stock_prices (list[dict]): Daily stock price data fetched from the API.

    Returns:
        Dict[str, Union[str, float, int]]: Processed latest stock price data for a single day.
    """
    # Get yesterday's date to fetch the most recent data
    # pendulum.yesterday() used, airflow scheduled to run every 4:15am gmt+8
    # so then yesterday() gets the right date.
    date = pendulum.yesterday().to_date_string()
    latest_entry = stock_prices.get(date, {})

    # If data is available for the latest entry, calculate the difference and return it
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
        # If no data is available for the latest date, return default values
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
    """
    DAG for extracting stock data from Alpha Vantage API, transforming it, and loading it into BigQuery.
    """

    @task
    def get_dataset_exists() -> bool:
        """
        Check if a dataset already exists in BigQuery.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
        try:
            hook.get_dataset(project_id=BQ_PROJECT, dataset_id=BQ_DATASET)
            return True
        except exceptions.NotFound:
            return False

    @task
    def create_dataset() -> bool:
        """
        Creates a dataset with the default table expiration set to 7 days in BigQuery.
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

        dataset_resourse = {
            "defaultTableExpirationMs": "604800000",  # 7 days in milliseconds
        }

        hook.create_empty_dataset(
            project_id=BQ_PROJECT,
            dataset_id=BQ_DATASET,
            exists_ok=True,  # This will prevent errors if the dataset already exists
            dataset_reference=dataset_resourse,
        )

        print(f"Dataset {BQ_DATASET} created or updated successfully.")
        return True

    @task
    def check_price_table_exists(symbol: str) -> bool:
        """
        Check if a table for the given stock symbol already exists in BigQuery.

        Args:
            symbol (str): The stock symbol to check for.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
        return hook.table_exists(
            project_id=BQ_PROJECT, dataset_id=BQ_DATASET, table_id=f"{symbol}_prices"
        )

    @task
    def create_price_table(symbol: str) -> None:
        """
        Create a BigQuery table for storing stock price data for the given symbol.

        Args:
            symbol (str): The stock symbol to create the table for.

        Returns:
            None
        """

        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
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

    @task(trigger_rule=TriggerRule.ALL_FAILED)
    def delete_price_table(symbol: str) -> None:
        """
        Delete the BigQuery table storing the stock price data for the given symbol.

        Args:
            symbol (str): The stock symbol to delete the table for

        Returns:
            None
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices"
        hook.delete_table(project_id=BQ_PROJECT, table_id=table_id)

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def upload_backfill_bq(symbol: str, data: pd.DataFrame) -> None:
        """
        Uploads price data to BigQuery for a given symbol.

        This task connects to BigQuery using a service account and loads
        the provided DataFrame into the specified table.

        Args:
            symbol (str): The stock symbol used to identify the table.
            data (pd.DataFrame): The DataFrame containing price data to upload.

        Returns:
            None
        """
        credentials = service_account.Credentials.from_service_account_file(
            GCP_SERVICE_JSON_LOC
        )
        client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{symbol}_prices"
        job = client.load_table_from_dataframe(data, table_id)
        job.result()

    @task
    def get_latest_row_date(symbol: str) -> pendulum.DateTime:
        """
        Retrieves the latest date from the price data of a given symbol in BigQuery.

        This task executes a SQL query to obtain the most recent date entry
        for the specified stock symbol's price data.

        Args:
            symbol (str): The stock symbol used to identify the price data table.

        Returns:
            pendulum.DateTime: The latest date from the price data.
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

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
    def insert_row_into_bq(data: dict, symbol: str) -> None:
        """
        Inserts a row into BigQuery using the insert_all method.

        This task logs the data being inserted and adds it to the
        specified BigQuery table for the given stock symbol.

        Args:
            data (dict): A dictionary containing the data for one row.
            symbol (str): The stock symbol used to identify the target BigQuery table.

        Returns:
            None
        """
        hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

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

    @task
    def fetch_stock_data(
        symbol: str, backfill: bool = False
    ) -> Union[Dict[str, Union[str, float, int]], pd.DataFrame]:
        """
        Fetches the latest stock data for a given symbol from the Alpha Vantage API.

        If backfill is True, the function returns historical data as a pandas DataFrame.
        Otherwise, it returns the latest available stock data as a dictionary.

        Args:
            symbol (str): The stock symbol for which to fetch data.
            backfill (bool, optional): Flag indicating whether to fetch historical data.
                                        Defaults to False.

        Returns:
            Union[Dict[str, Union[str, float, int]], pd.DataFrame]:
                - If backfill is True, returns a DataFrame containing historical stock data.
                - If backfill is False, returns a dictionary with the latest stock data.
        """
        base_url, params = _build_alpha_api_url(symbol, backfill=backfill)

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

    get_dataset = get_dataset_exists.override(task_id="check_dataset_exists")()

    @task.branch(task_id="check_dataset_exists")
    def check_dataset(ti=None) -> str:
        dataset_exists = ti.xcom_pull(task_ids="check_dataset_exists")

        if dataset_exists:
            return "stock_prices_operation"

        else:
            return "create_dataset"

    check_dataset = check_dataset()

    get_dataset >> check_dataset

    create_dataset = create_dataset()

    with TaskGroup(group_id="stock_prices_operation") as stock_prices:

        for symbol in ["AAPL", "MSFT"]:

            ################
            check_price_table = check_price_table_exists.override(
                task_id=f"check_{symbol}_prices_exists"
            )(symbol)

            price_join_1 = EmptyOperator(
                task_id=f"{symbol}_end_task",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            @task.branch(
                task_id=f"branch_1_{symbol}", trigger_rule=TriggerRule.ONE_SUCCESS
            )
            def branch_1(dataset_just_created: bool, symbol=symbol, ti=None) -> str:

                if dataset_just_created:
                    return f"stock_prices_operation.create_backfill_{symbol}_group"

                price_table_exists = ti.xcom_pull(
                    task_ids=f"stock_prices_operation.check_{symbol}_prices_exists"
                )

                if price_table_exists:
                    return f"stock_prices_operation.get_latest_entry_{symbol}_price"
                else:
                    return f"stock_prices_operation.create_backfill_{symbol}_group"

            b1 = branch_1(create_dataset)

            check_price_table >> b1

            ################

            with TaskGroup(
                group_id=f"create_backfill_{symbol}_group"
            ) as create_backfill:
                """
                TaskGroup for handling the backfill process of stock price data for a specific symbol.

                This TaskGroup performs the following tasks:
                1. Creates a price table in BigQuery if it does not exist.
                2. Fetches historical stock data for the given symbol.
                3. Uploads the fetched data to the created price table in BigQuery.
                4. Deletes the old price table after successful upload.

                Tasks within this group:
                - `inner_create_{symbol}_prices_table`: Creates the price table.
                - `inner_fetch_{symbol}_backfill`: Fetches historical stock data.
                - `inner_upload_{symbol}_backfill_bq`: Uploads data to BigQuery.
                - `inner_delete_{symbol}_prices_table`: Deletes the old price table.

                Parameters:
                    symbol (str): The stock symbol for which the backfill process is being performed.
                """

                create_table = create_price_table.override(
                    task_id=f"inner_create_{symbol}_prices_table"
                )(symbol=symbol)

                data = fetch_stock_data.override(
                    task_id=f"inner_fetch_{symbol}_backfill"
                )(symbol=symbol, backfill=True)

                upload = upload_backfill_bq.override(
                    task_id=f"inner_upload_{symbol}_backfill_bq"
                )(symbol=symbol, data=data)

                delete = delete_price_table.override(
                    task_id=f"inner_delete_{symbol}_prices_table"
                )(symbol)

                [create_table, data] >> upload
                create_table >> delete

            latest_entry = get_latest_row_date.override(
                task_id=f"get_latest_entry_{symbol}_price"
            )(symbol)

            b1 >> Label("Table doesnt exists") >> create_backfill >> price_join_1
            b1 >> Label("Table exists") >> latest_entry

            ################

            @task.branch()
            def branch_2(symbol=symbol, ti=None) -> str:
                latest_date = ti.xcom_pull(
                    task_ids=f"stock_prices_operation.get_latest_entry_{symbol}_price"
                )

                if latest_date == pendulum.yesterday().to_date_string():
                    return f"stock_prices_operation.do_nothing_{symbol}"

                else:
                    return f"stock_prices_operation.fetch_insert_{symbol}"

            b2 = branch_2.override(task_id=f"branch_2_{symbol}")()

            latest_entry >> b2

            ################

            with TaskGroup(group_id=f"fetch_insert_{symbol}") as fetch_insert:
                """
                TaskGroup for fetching the latest stock data and inserting it into BigQuery.

                This TaskGroup performs the following tasks for the specified stock symbol:
                1. Fetches the latest stock data using the Alpha Vantage API.
                2. Inserts the fetched data into the corresponding BigQuery table.

                Tasks within this group:
                - `inner_fetch_latest_{symbol}`: Fetches the latest stock data.
                - `inner_insert_latest_{symbol}`: Inserts the fetched data into BigQuery.

                Parameters:
                    symbol (str): The stock symbol for which to fetch and insert data.
                """

                data = fetch_stock_data.override(
                    task_id=f"inner_fetch_latest_{symbol}"
                )(symbol=symbol, backfill=False)

                insert_job = insert_row_into_bq.override(
                    task_id=f"inner_insert_latest_{symbol}"
                )(data=data, symbol=symbol)

                data >> insert_job

            do_nothing = EmptyOperator(task_id=f"do_nothing_{symbol}")

            (
                b2
                >> Label(f"{symbol}_prices not up to date")
                >> fetch_insert
                >> price_join_1
            )
            (b2 >> Label(f"{symbol}_prices up to date") >> do_nothing >> price_join_1)

        check_dataset >> create_dataset
        check_dataset >> stock_prices


etl()
