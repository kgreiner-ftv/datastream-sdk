import logging
import azure.functions as func


def main(request: func.HttpRequest):
    logging.info(
        f"Python blob trigger function processing blob"
    )
    return func.HttpResponse("Success.")



