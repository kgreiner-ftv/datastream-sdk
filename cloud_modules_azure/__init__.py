import logging
import run_aggregations
import json
import os
import datetime
import azure.cosmos.cosmos_client as cosmos_client
import azure.functions as func


def main(myblob: func.InputStream, resultdoc: func.Out[func.DocumentList]):
    # def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processing blob \n"
        f"Name: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
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

    result = run_aggregations.main(None, myblob, cloud="azure")

    logging.info(f"result:{result}")

    container = db_connection(cosmos_db_end_point, cosmos_db_primary_key, cosmos_db_database_name,
                              cosmos_db_container_name)
    upsert_items_into_cosmos_db(container, cosmos_db_container_name, result)

    logging.info(json.dumps(result, indent=2))

    result = update_result(result)

    logging.info(json.dumps(result, indent=2))

    resultdoc.set(func.DocumentList(result))


def update_result(result):
    """
    remove unique_visitors_value from list of dictionary, since we don't require this.
    :param result:
    :return:
    """
    length = len(result)
    logging.info(f"result >> length :{length}")

    for i in range(length):
        del result[i]["unique_visitors_value"]
    return result


def calculate_delta(existing_unique_visitor_list, current_unique_visitor_list):
    current_unique_visitor_set = set(current_unique_visitor_list)
    existing_unique_visitor_set = set(existing_unique_visitor_list)
    delta_set = current_unique_visitor_set - existing_unique_visitor_set
    return list(delta_set)


def upsert_items_into_cosmos_db(container, container_name, result):
    """
    upsert items into cosmos db, if same time_stamp exist then update the unique visitors and unique_visitors_value
    :param container:
    :param container_name:
    :param result:
    :return:
    """
    length = len(result)
    for i in range(length):

        # fetch the unique visitors from cosmos db for the given time stamp
        existing_unique_visitor_list = fun_get_unique_visitors_from_db(container, container_name,
                                                                       str(result[i].get("start_timestamp")))

        if existing_unique_visitor_list is None:
            existing_unique_visitor_list = []

        # Get the unique visitors from result
        current_unique_visitor_list = result[i].get("unique_visitors_value")

        # traverse over current_unique_visitor_list and append to existing_unique_visitor_list if the tuple doesn't
        # exist in existing_unique_visitor_list
        delta_list = calculate_delta(existing_unique_visitor_list, current_unique_visitor_list)

        if delta_list is not None and len(delta_list) > 0:
            existing_unique_visitor_list.extend(delta_list)

        # update the result with updated existing_unique_visitor_list
        result[i]["unique_visitors_value"] = existing_unique_visitor_list

        container.upsert_item({
            'id': str(result[i].get("start_timestamp")),
            'date_and_time_in_utc': datetime.datetime.fromtimestamp(result[i].get("start_timestamp")).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'unique_visitors': len(existing_unique_visitor_list),
            'unique_visitors_value': result[i]["unique_visitors_value"]
        }
        )


def fun_get_unique_visitors_from_db(container, container_name, time_stamp):
    """
    fetch the unique_visitors for given time_stamp from cosmos db
    :param container:
    :param container_name:
    :param time_stamp:
    :return:
    """
    for item in container.query_items(
            query='SELECT * FROM ' + container_name + ' r WHERE r.id =' + '\'' + time_stamp + '\'',
            enable_cross_partition_query=True):
        return [tuple(x) for x in item["unique_visitors_value"]]


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
