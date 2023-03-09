import logging
import run_aggregations
import json
import os
from common_module.database_util import db_connection
from .common_utils import calculate_delta, create_document, query_item_from_db, update_result, \
    upsert_items_into_cosmos_db
import azure.functions as func


def main(myblob: func.InputStream, resultdoc: func.Out[func.DocumentList]):
    logging.info(
        f"Python blob trigger function processing blob \n"
        f"Name: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    # cosmos_db_end_point = os.environ["COSMOS_DB_ENDPOINT"]
    # cosmos_db_primary_key = os.environ["COSMOS_DB_PRIMARY_KEY"]
    # cosmos_db_database_name = os.environ["COSMOS_DATABASE_NAME"]

    result = run_aggregations.main(None, myblob, cloud="azure")

    ingest_data(result)

    logging.info(json.dumps(result, indent=2))

    result = update_result(result)

    logging.info(json.dumps(result, indent=2))

    resultdoc.set(func.DocumentList(result))


def ingest_data(result):
    logging.info(f"result:{result}")

    cosmos_db_container_name = os.environ["COSMOS_CONTAINER_NAME"]
    logging.info(f"cosmos_db_container_name: {cosmos_db_container_name}")

    container = db_connection("cosmos")
    upsert_items_into_cosmos_db(container, cosmos_db_container_name, result)

# def update_result(result):
#     """
#     remove unique_visitors_value from list of dictionary, since we don't require this.
#     :param result:
#     :return:
#     """
#     length = len(result)
#     logging.info(f"result >> length :{length}")
#
#     for i in range(length):
#         del result[i]["unique_visitors_value"]
#     return result


# def upsert_items_into_cosmos_db(container, container_name, result):
#     """
#     upsert items into cosmos db, if same time_stamp exist then update the unique visitors and unique_visitors_value
#     :param container:
#     :param container_name:
#     :param result:
#     :return:
#     """
#     length = len(result)
#     for i in range(length):
#
#         unique_visitors_value = result[i].get("unique_visitors_value")
#         logline_date = datetime.datetime.fromtimestamp(result[i].get("start_timestamp")).date().isoformat()
#
#         for item in unique_visitors_value:
#             user_agent = item[0]
#             client_ip = item[1]
#             last_octet = client_ip.split('.')[-1]
#             partition_key_value = logline_date + '_' + last_octet
#             ua_cip_list_tuple = [(user_agent, client_ip)]
#
#             query_as_list = query_item_from_db(container, container_name, logline_date,
#                                                last_octet, partition_key_value)
#
#             if len(query_as_list) == 0:
#                 document_id = str(uuid.uuid4())
#                 logging.info(f"document id >>  :{document_id}")
#                 document = create_document(document_id, partition_key_value, logline_date, last_octet,
#                                            ua_cip_list_tuple)
#                 container.create_item(body=document, partition_key=partition_key_value)
#             else:
#                 unique_visitor_value_list = query_as_list[0]["unique_visitor_value"]
#                 delta_list = calculate_delta(unique_visitor_value_list, ua_cip_list_tuple)
#                 if delta_list is not None and len(delta_list) > 0:
#                     unique_visitor_value_list.extend(delta_list)
#                     document = create_document(query_as_list[0]["id"], query_as_list[0]["partition_key"],
#                                                logline_date, last_octet,
#                                                unique_visitor_value_list)
#                     container.upsert_item(body=document, partition_key=partition_key_value)
#

# def create_document(document_id, partition_key_value, logline_date, last_octet, ua_cip_list_tuple):
#     """
#
#     :param document_id:
#     :param partition_key_value:
#     :param logline_date:
#     :param last_octet:
#     :param ua_cip_list_tuple:
#     :return:
#     """
#     document = {
#         'id': document_id,
#         'partition_key': partition_key_value,
#         "date": logline_date,
#         "last_octet": last_octet,
#         "unique_visitor_value": ua_cip_list_tuple
#     }
#     return document
#

# def query_item_from_db(container, container_name, logline_date, last_octet, partition_key_value):
#     """
#     fetch the unique_visitors for given time_stamp from cosmos db
#     :param partition_key_value:
#     :param last_octet:
#     :param logline_date:
#     :param container:
#     :param container_name:
#     :return:
#     """
#     date_and_octet_query = f"SELECT *  FROM {container_name} c WHERE c.date = '{logline_date}' and c.last_octet = '{last_octet}'"
#     octet_query_result = container.query_items(query=date_and_octet_query, partition_key=partition_key_value)
#     octet_query_result_as_list = list(octet_query_result)
#     return octet_query_result_as_list


# def calculate_delta(existing_unique_visitor_list, current_unique_visitor_list):
#     """
#     calculate delta from current unique visitors list
#     :param existing_unique_visitor_list:
#     :param current_unique_visitor_list:
#     :return:
#     """
#     existing_unique_visitor_list = [tuple(x) for x in existing_unique_visitor_list]
#     current_unique_visitor_set = set(current_unique_visitor_list)
#     existing_unique_visitor_set = set(existing_unique_visitor_list)
#     delta_set = current_unique_visitor_set - existing_unique_visitor_set
#     return list(delta_set)


# def db_connection(url_connection, cosmos_db_primary_key, cosmos_db_database_name, cosmos_db_container_name):
#     """
#     connect to cosmosdb
#     :param url_connection:
#     :param cosmos_db_primary_key:
#     :param cosmos_db_database_name:
#     :param cosmos_db_container_name:
#     :return:
#     """
#     auth = {"masterKey": cosmos_db_primary_key}
#     client = cosmos_client.CosmosClient(url_connection, auth)
#     database = client.get_database_client(cosmos_db_database_name)
#     container = database.get_container_client(cosmos_db_container_name)
#     return container
