import logging
import json
import os
from .utils import get_date_list, db_connection, query_item_from_db
import azure.functions as func


def main(request: func.HttpRequest) -> func.HttpResponse:
    logging.info(
        f"Unique visitor function will get trigger on http request"
    )

    from_date = request.get_json()["from_date"]
    to_date = request.get_json()["to_date"]
    logging.info(f'Request body : from_date: {from_date} and to_date :{to_date}')

    date_list = []

    try:
        date_list = get_date_list(from_date, to_date)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)

    result = calc_unique_visitor(date_list)
    json_str = json.dumps(result)
    return func.HttpResponse(json_str, mimetype="application/json")


def calc_unique_visitor(date_list):
    """
    :param date_list:
    :return:
    """
    cosmos_db_end_point = os.environ["COSMOS_DB_ENDPOINT"]
    cosmos_db_primary_key = os.environ["COSMOS_DB_PRIMARY_KEY"]
    cosmos_db_database_name = os.environ["COSMOS_DATABASE_NAME"]
    cosmos_db_container_name = os.environ["COSMOS_CONTAINER_NAME"]

    logging.info(
        f"cosmos_db_end_point:{cosmos_db_end_point}\n"
        f"cosmos_db_primary_key: {cosmos_db_primary_key}\n"
        f"cosmos_db_database_name: {cosmos_db_database_name}\n"
        f"cosmos_db_container_name: {cosmos_db_container_name}")

    container = db_connection(cosmos_db_end_point, cosmos_db_primary_key, cosmos_db_database_name,
                              cosmos_db_container_name)
    return get_result(container, cosmos_db_container_name, date_list)


def get_result(container, container_name, logline_date_list):
    """

    :param container:
    :param container_name:
    :param logline_date_list:
    :return:
    """
    response = {}
    logging.info(f'Request body list: {logline_date_list}')

    for logline_date in logline_date_list:
        query = f"SELECT DISTINCT CONCAT(val[0],',',val[1]) FROM {container_name} c JOIN val IN c.unique_visitor_value WHERE c.date = '{logline_date}'"
        count = query_item_from_db(container, query)
        response[logline_date] = count

    return response