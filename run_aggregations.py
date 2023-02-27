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
import textwrap
import logging
import time
import json
import os
#import azure.cosmos.cosmos_client as cosmos_client

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
        default=os.getcwd() + "/sample-input/test-data-custom.gz",
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
    max_unique_visitors = 0
    for item in container.query_items(
            query='SELECT * FROM ' + container_name + ' r WHERE r.id =' + '\'' + time_stamp + '\'',
            enable_cross_partition_query=True):
        max_unique_visitors = max(max_unique_visitors, int(item["unique_visitors"]))
    return max_unique_visitors


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

    # length = len(obj.result_map)
    # print(obj.result_map)

    # final_result = {}

    # for i in range(length):
    # time_stamp = obj.result_map[i].get("start_timestamp")
    # final_result[time_stamp] = obj.result_map[i]

    # publish results

    # config = {
    #     "endpoint": "https://aduvesdkcosmos.documents.azure.com:443/",
    #     "primarykey": "G4oUmN5QwMOp6J1dffFjgbUygiWijBWZehQmGHV5MgTjTigaZLZUeeY14rvpmeWsoZwYLyaiTSp1ACDb64hhQQ=="
    # }
    #
    # url_connection = config["endpoint"]
    # auth = {"masterKey": config["primarykey"]}
    # # Create the cosmos client
    # client = cosmos_client.CosmosClient(url_connection, auth)
    #
    # database_name = 'aduvesdkdb'
    #
    # database = client.get_database_client(database_name)
    #
    # CONTAINER_NAME = 'aduvesdkcontainer'
    # container = database.get_container_client(CONTAINER_NAME)
    #
    # length = len(obj.result_map)
    # for i in range(length):
    #     unv = int(fun_get_unique_visitors(container, CONTAINER_NAME, str(obj.result_map[i].get("start_timestamp"))))
    #     if obj.result_map[i].get("unique_visitors") < unv:
    #         obj.result_map[i]["unique_visitors"] = unv
    #     container.upsert_item({
    #         'id': str(obj.result_map[i].get("start_timestamp")),
    #         'unique_visitors': str(obj.result_map[i].get("unique_visitors")),
    #         'value': obj.result_map[i]
    #     }
    #     )

    return obj.result_map
    # return final_result


if __name__ == "__main__":
    result = main(None, None, None)
    print("Result...")
    print(json.dumps(result, indent=2))
