import logging
from datetime import datetime, timedelta


def query_item_from_db(container, query):
    """
    fetch the unique_visitor count  for given logline date from cosmos db
    :param query:
    :param container:
    :return:
    """
    logging.info(f"query:{query}")
    octet_query_result = container.query_items(query=query, enable_cross_partition_query=True)
    total_visitor_count = len(list(octet_query_result))
    logging.info(f"total_visitor_count:{total_visitor_count}")
    return total_visitor_count


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

    delta = end_date - start_date
    days_diff = delta.days

    if days_diff > 90:
        raise Exception("Error: The difference between the two dates is not more than 90 days.")

    # get today's date
    today = datetime.utcnow()

    if end_date > today:
        raise Exception("Error: to_date cannot be later than today.")

    date_list = []
    while start_date <= end_date:
        date_list.append(start_date.strftime(date_format))
        start_date += timedelta(days=1)

    date_list.reverse()

    logging.info(f"logline date list :: {date_list}")

    return date_list
