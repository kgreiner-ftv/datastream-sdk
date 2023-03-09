import logging
from datetime import datetime, timedelta
import azure.cosmos.cosmos_client as cosmos_client


def query_item_from_db(container, query):
    """
    fetch the unique_visitor count  for given logline date from cosmos db
    :param query:
    :param container:
    :return:
    """
    #query = f"SELECT DISTINCT CONCAT(val[0],',',val[1]) FROM {container_name} c JOIN val IN c.unique_visitor_value WHERE c.date = '{logline_date}'"
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


def get_date_list(from_date, to_date):
    """

    :param from_date:
    :param to_date:
    :return:
    """
    logging.info(f"Get list of date from {from_date} and {to_date}.")

    date_format = "%Y-%m-%d"
    if not from_date or not to_date:
        raise Exception("Error: from_date and to_date cannot be empty")

    try:
        start_date = datetime.strptime(from_date, date_format)
        end_date = datetime.strptime(to_date, date_format)
    except ValueError:
        raise Exception("Error: from_date or to_date has an invalid date format. Please enter the date in the format "
                        "of YYYY-MM-DD.")

    if end_date < start_date:
        raise Exception("Error: to_date cannot be less than from_date")

    date_list = []
    while start_date <= end_date:
        date_list.append(start_date.strftime(date_format))
        start_date += timedelta(days=1)

    return date_list
