import logging
import run_aggregations
import json
import os
import azure.cosmos.cosmos_client as cosmos_client
import azure.functions as func


def main(myblob: func.InputStream, resultdoc: func.Out[func.DocumentList]):
    logging.info(
        f"Python blob trigger function processing blob \n"
        f"Name: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    result = run_aggregations.main(None, myblob, cloud="azure")

    cosmos_db_end_point = os.environ["COSMOS_DB_ENDPOINT"]
    cosmos_db_primary_key = os.environ["COSMOS_DB_PRIMARY_KEY"]
    cosmos_db_database_name = os.environ["COSMOS_DATABASE_NAME"]
    cosmos_db_container_name = os.environ["COSMOS_CONTAINER_NAME"]

    logging.info("cosmos_db_end_point", cosmos_db_end_point)
    logging.info("cosmos_db_primary_key", cosmos_db_primary_key)
    logging.info("cosmos_db_database_name", cosmos_db_database_name)
    logging.info("cosmos_db_container_name", cosmos_db_container_name)

    container = db_connection(cosmos_db_end_point, cosmos_db_primary_key, cosmos_db_database_name,
                              cosmos_db_container_name)
    upsert_items_into_cosmos_db(container, cosmos_db_container_name, result)

    logging.info(json.dumps(result, indent=2))
    resultdoc.set(func.DocumentList(result))


def upsert_items_into_cosmos_db(container, container_name, result):
    length = len(result)
    for i in range(length):
        unv = int(fun_get_unique_visitors(container, container_name, str(result[i].get("start_timestamp"))))
        if result[i].get("unique_visitors") < unv:
            result[i]["unique_visitors"] = unv
        container.upsert_item({
            'id': str(result[i].get("start_timestamp")),
            'unique_visitors': str(result[i].get("unique_visitors")),
            'value': result[i]
        }
        )


def fun_get_unique_visitors(container, container_name, time_stamp):
    """
    fetch the unique_visitors for given time_stamp from cosmos db
    :param container:
    :param container_name:
    :param time_stamp:
    :return:
    """
    max_unique_visitors = 0
    for item in container.query_items(
            query='SELECT * FROM ' + container_name + ' r WHERE r.id =' + '\'' + time_stamp + '\'',
            enable_cross_partition_query=True):
        max_unique_visitors = max(max_unique_visitors, int(item["unique_visitors"]))
    return max_unique_visitors


def db_connection(url_connection, cosmos_db_primary_key, cosmos_db_database_name, cosmos_db_container_name):
    auth = {"masterKey": cosmos_db_primary_key}
    client = cosmos_client.CosmosClient(url_connection, auth)
    database = client.get_database_client(cosmos_db_database_name)
    container = database.get_container_client(cosmos_db_container_name)
    return container
