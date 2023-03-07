# Copyright 2020 Akamai Technologies, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Main Module
"""
# TODO: add more info

import argparse
import datetime
import json
import logging
import os
import textwrap
import time
import uuid

import azure.cosmos.cosmos_client as cosmos_client
from aggregation_modules.aggregator import Aggregator


def parse_inputs() -> dict:
    """
    parse the input command line arguments
    and return dictionary
    """
    # TODO: add details of the code functionality
    parser = argparse.ArgumentParser(
        prog=__file__,
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent(
            """\
            Helps aggregate data
            """
        ),
    )

    parser.add_argument(
        "--loglevel",
        default="info",
        type=str,
        choices=["critical", "error", "warn", "info", "debug"],
        help=textwrap.dedent(
            """\
            logging level.
            (default: %(default)s)
            \n"""
        ),
    )

    parser.add_argument(
        "--input",
        default=os.getcwd() + "/sample-input/test-data-custom_test_march06.gz",
        type=str,
        help=textwrap.dedent(
            """\
            specify the input file to aggregate.
            (default: %(default)s)
            \n"""
        ),
    )

    args, _ = parser.parse_known_args()
    return vars(args)


def init_logging(log_level):
    """
    creates a logger
    """
    log_levels = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=log_levels[log_level],
        format="%(process)5d| "
               + "%(asctime)s| "
               + "%(levelname)8s| "
               + "%(name)s:[%(funcName)s]:%(lineno)d|"
               + " %(message)s",
    )
    # create logger
    logger = logging.getLogger()

    return logger


def fun_get_unique_visitors(container, container_name, time_stamp):
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


def calculate_delta1(existing_unique_visitor_list, current_unique_visitor_list):
    current_unique_visitor_set = set(current_unique_visitor_list)
    existing_unique_visitor_set = set(existing_unique_visitor_list)
    delta_set = current_unique_visitor_set - existing_unique_visitor_set
    return list(delta_set)


def main(aws_event, azure_blob, cloud=None):
    """
    main function
    """
    params = parse_inputs()

    # reset defaults
    # params["loglevel"] = "info"

    logger = init_logging(params["loglevel"])
    logger.debug("logging level set to %s mode", params["loglevel"])

    # init
    obj = Aggregator(cloud_provider=cloud)

    # parse config files
    logger.debug("read metadata files...")
    obj.read_metadata()

    # set input data
    input_file = None
    input_bucket = None

    if cloud is None:
        # temporarily setting the input file
        input_file = params["input"]

    if cloud == "aws":
        input_bucket = aws_event["Records"][0]["s3"]["bucket"]["name"]
        input_file = aws_event["Records"][0]["s3"]["object"]["key"]

    if cloud == "azure":
        input_file = azure_blob

    # parse input data
    logger.debug("read input files...")
    obj.read_input_data(input_file=input_file, bucket_name=input_bucket)

    # process input data
    logger.debug("process input files...")
    obj.process_data()

    # uncomment the below line to test in local with cosmos database
    #test_with_cosmos_db(obj.result_map)

    # length = len(obj.result_map)
    # for i in range(length):
    # del obj.result_map[i]["unique_visitors_value"]

    # publish results
    return obj.result_map


def test_with_cosmos_db(result):
    config = {
        "endpoint": "https://aduvesdkcosmos.documents.azure.com:443/",
        "primarykey": "G4oUmN5QwMOp6J1dffFjgbUygiWijBWZehQmGHV5MgTjTigaZLZUeeY14rvpmeWsoZwYLyaiTSp1ACDb64hhQQ=="
    }

    url_connection = config["endpoint"]
    auth = {"masterKey": config["primarykey"]}
    # Create the cosmos client
    client = cosmos_client.CosmosClient(url_connection, auth)
    database_name = 'aduvesdkdb'
    database = client.get_database_client(database_name)
    CONTAINER_NAME = 'testcontainer'
    container = database.get_container_client(CONTAINER_NAME)

    length = len(result)

    for i in range(length):

        unique_visitors_value = result[i].get("unique_visitors_value")
        logline_date = datetime.datetime.fromtimestamp(result[i].get("start_timestamp")).date().isoformat()

        for item in unique_visitors_value:
            user_agent = item[0]
            client_ip = item[1]
            last_octet = client_ip.split('.')[-1]
            partition_key_value = logline_date + '_' + last_octet
            ua_cip_list_tuple = [(user_agent, client_ip)]

            query_as_list = query_item_from_db(container, CONTAINER_NAME, logline_date,
                                               last_octet, partition_key_value)

            if len(query_as_list) == 0:
                document_id = str(uuid.uuid4())
                logging.info(f"document id >>  :{document_id}")
                document = create_document(document_id, partition_key_value, logline_date, last_octet,
                                           ua_cip_list_tuple)
                container.create_item(body=document, partition_key=partition_key_value)
            else:
                unique_visitor_value_list = query_as_list[0]["unique_visitor_value"]
                res = calculate_delta(unique_visitor_value_list, ua_cip_list_tuple)
                if res is not None and len(res) > 0:
                    unique_visitor_value_list.extend(res)
                    document = create_document(query_as_list[0]["id"], query_as_list[0]["partition_key"],
                                               logline_date, last_octet,
                                               unique_visitor_value_list)
                    container.upsert_item(body=document, partition_key=partition_key_value)


def create_document(document_id, partition_key_value, logline_date, last_octet, ua_cip_list_tuple):
    """

    :param document_id:
    :param partition_key_value:
    :param logline_date:
    :param last_octet:
    :param ua_cip_list_tuple:
    :return:
    """
    document = {
        'id': document_id,
        'partition_key': partition_key_value,
        "date": logline_date,
        "last_octet": last_octet,
        "unique_visitor_value": ua_cip_list_tuple
    }
    return document


def query_item_from_db1(container, container_name, logline_date):
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

def query_item_from_db(container, container_name, logline_date, last_octet, partition_key_value):
    """
    fetch the unique_visitors for given time_stamp from cosmos db
    :param partition_key_value:
    :param logline_date:
    :param last_octet:
    :param container:
    :param container_name:
    :return:
    """
    date_and_octet_query = f"SELECT *  FROM {container_name} c WHERE c.date = '{logline_date}' and c.last_octet = '{last_octet}'"
    octet_query_result = container.query_items(query=date_and_octet_query, partition_key=partition_key_value)
    octet_query_result_as_list = list(octet_query_result)
    return octet_query_result_as_list


def calculate_delta(existing_unique_visitor_list, current_unique_visitor_list):
    """
    calculate delta from current unique visitors list
    :param existing_unique_visitor_list:
    :param current_unique_visitor_list:
    :return:
    """
    existing_unique_visitor_list = [tuple(x) for x in existing_unique_visitor_list]
    current_unique_visitor_set = set(current_unique_visitor_list)
    existing_unique_visitor_set = set(existing_unique_visitor_list)
    delta_set = current_unique_visitor_set - existing_unique_visitor_set
    return list(delta_set)


if __name__ == "__main__":
    result = main(None, None, None)
    print("Result...")
    print(json.dumps(result, indent=2))
