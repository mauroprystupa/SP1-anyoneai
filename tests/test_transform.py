import json

import pandas as pd
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from src.config import (
    DATASET_ROOT_PATH,
    PUBLIC_HOLIDAYS_URL,
    QUERY_RESULTS_ROOT_PATH,
    get_csv_to_table_mapping,
)
from src.extract import extract
from src.load import load
from src.transform import (
    QueryResult,
    query_delivery_date_difference,
    query_freight_value_weight_relationship,
    query_global_ammount_order_status,
    query_orders_per_day_and_holidays_2017,
    query_real_vs_estimated_delivered_time,
    query_revenue_by_month_year,
    query_revenue_per_state,
    query_top_10_least_revenue_categories,
    query_top_10_revenue_categories,
)


@fixture(scope="session", autouse=True)
def database() -> Engine:
    """Initialize the database for testing."""
    engine = create_engine("sqlite://")
    csv_folder = DATASET_ROOT_PATH
    public_holidays_url = PUBLIC_HOLIDAYS_URL
    csv_table_mapping = get_csv_to_table_mapping()
    csv_dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
    load(data_frames=csv_dataframes, database=engine)
    return engine


def read_query_result(query_name: str) -> dict:
    """Read the query from the json file.
    Args:
        query_name (str): The name of the query.
    Returns:
        dict: The query as a dictionary.
    """
    with open(f"{QUERY_RESULTS_ROOT_PATH}/{query_name}.json", "r") as f:
        query_result = json.load(f)

    return query_result


def pandas_to_json_object(df: pd.DataFrame) -> dict:
    """Convert pandas dataframe to json object.
    Args:
        df (pd.DataFrame): The dataframe.
    Returns:
        dict: The dataframe as a json object.
    """
    return json.loads(df.to_json(orient="records"))


def test_query_revenue_by_month_year(database: Engine):
    query_name = "revenue_by_month_year"
    actual: QueryResult = query_revenue_by_month_year(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_delivery_date_difference(database: Engine):
    query_name = "delivery_date_difference"
    actual: QueryResult = query_delivery_date_difference(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_global_ammount_order_status(database: Engine):
    query_name = "global_ammount_order_status"
    actual: QueryResult = query_global_ammount_order_status(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_revenue_per_state(database: Engine):
    query_name = "revenue_per_state"
    actual: QueryResult = query_revenue_per_state(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_top_10_least_revenue_categories(database: Engine):
    query_name = "top_10_least_revenue_categories"
    actual: QueryResult = query_top_10_least_revenue_categories(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_top_10_revenue_categories(database: Engine):
    query_name = "top_10_revenue_categories"
    actual: QueryResult = query_top_10_revenue_categories(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_real_vs_estimated_delivered_time(database: Engine):
    query_name = "real_vs_estimated_delivered_time"
    actual: QueryResult = query_real_vs_estimated_delivered_time(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_orders_per_day_and_holidays_2017(database: Engine):
    query_name = "orders_per_day_and_holidays_2017"
    actual: QueryResult = query_orders_per_day_and_holidays_2017(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected


def test_query_get_freight_value_weight_relationship(database: Engine):
    query_name = "get_freight_value_weight_relationship"
    actual: QueryResult = query_freight_value_weight_relationship(database)
    expected = read_query_result(query_name)
    assert pandas_to_json_object(actual.result) == expected
