import logging
import os
import json
from datetime import datetime, timedelta
import azure.cosmos.cosmos_client as cosmos_client
import azure.functions as func


def main(request: func.HttpRequest):
    logging.info(
        f"Unique visitor function will get trigger on http request"
    )

    cosmos_db_end_point = os.environ["COSMOS_DB_ENDPOINT"]
    cosmos_db_primary_key = os.environ["COSMOS_DB_PRIMARY_KEY"]
    cosmos_db_database_name = os.environ["COSMOS_DATABASE_NAME"]
    cosmos_db_container_name = os.environ["COSMOS_CONTAINER_NAME"]

    logging.info(
        f"cosmos_db_end_point:{cosmos_db_end_point}\n"
        f"cosmos_db_primary_key: {cosmos_db_primary_key}\n"
        f"cosmos_db_database_name: {cosmos_db_database_name}\n"
        f"cosmos_db_container_name: {cosmos_db_container_name}"
    )

    container = db_connection(cosmos_db_end_point, cosmos_db_primary_key, cosmos_db_database_name,
                              cosmos_db_container_name)

    from_date = request.get_json()["from_date"]
    to_date = request.get_json()["to_date"]

    date_list = []

    try:
        date_list = get_date_list(from_date, to_date)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)

    logging.info(f'Request body : {date_list}')

    result = get_result(container, cosmos_db_container_name, date_list)
    json_str = json.dumps(result)

    return func.HttpResponse(json_str, mimetype="application/json")


def get_date_list(from_date, to_date):
    """

    :param from_date:
    :param to_date:
    :return:
    """
    date_format = "%Y-%m-%d"
    if not from_date or not to_date:
        raise ValueError("Error: from_date and to_date cannot be empty")

    try:
        start_date = datetime.strptime(from_date, date_format)
        end_date = datetime.strptime(to_date, date_format)
    except ValueError:
        raise ValueError("Error: from_date or to_date has an invalid date format")

    if end_date < start_date:
        raise ValueError("Error: to_date cannot be less than from_date")

    date_list = []
    while start_date <= end_date:
        date_list.append(start_date.strftime(date_format))
        start_date += timedelta(days=1)

    return date_list


def get_result(container, container_name, logline_date_list):
    response = {}
    logging.info(f'Request body list: {logline_date_list}')
    for logline_date in logline_date_list:
        count = query_item_from_db(container, container_name, logline_date)
        response[logline_date] = count

    return response


def query_item_from_db(container, container_name, logline_date):
    """
    fetch the unique_visitor count  for given logline date from cosmos db
    :param logline_date:
    :param container:
    :param container_name:
    :return:
    """
    query = f"SELECT DISTINCT CONCAT(val[0],',',val[1]) FROM {container_name} c JOIN val IN c.unique_visitor_value WHERE c.date = '{logline_date}'"
    logging.info(f"query:{query}")
    octet_query_result = container.query_items(query=query, enable_cross_partition_query=True)
    total_visitor_count = len(list(octet_query_result))
    logging.info(f"total_visitor_count:{total_visitor_count}")
    return total_visitor_count


def db_connection(url_connection, cosmos_db_primary_key, cosmos_db_database_name, cosmos_db_container_name):
    """
    connect to cosmosdb
    :param url_connection:
    :param cosmos_db_primary_key:
    :param cosmos_db_database_name:
    :param cosmos_db_container_name:
    :return:
    """
    auth = {"masterKey": cosmos_db_primary_key}
    client = cosmos_client.CosmosClient(url_connection, auth)
    database = client.get_database_client(cosmos_db_database_name)
    container = database.get_container_client(cosmos_db_container_name)
    return container
