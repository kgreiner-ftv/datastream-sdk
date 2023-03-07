import logging
import os
import azure.cosmos.cosmos_client as cosmos_client
import azure.functions as func


def main(request: func.HttpRequest):
    logging.info(
        f"Python blob trigger function processing blob"
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

    #req_body = request.get_body().decode('utf-8')

    logline_dates = request.get_json()["logline_date"]

    logging.info(f'Request body : {logline_dates}')

    result = get_result(container, cosmos_db_container_name, logline_dates)

    return func.HttpResponse(body=result)


def get_result(container, container_name, logline_dates):
    response = {}
    logline_date_list = logline_dates.split(',')
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
