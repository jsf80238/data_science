from pendulum import date, datetime, now
from http import HTTPStatus
import json
import os
# Imports above are standard Python
# Imports below are 3rd-party
from dateutil.parser import parse
from flask import Flask, Response
# Imports below are custom
from lib.base import Logger
from lib.stock import get_price, get_volume, STOCK_SERVICE_URL, STOCK_SERVICE_PORT

ERROR_MESSAGE = "Error message"
JSON_MIMETYPE = "application/json"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

app = Flask(__name__)
logger = Logger().get_logger()


@app.route("/<ticker>/<the_date>")
def return_quote(ticker: str, the_date: [str, date]) -> Response:
    if isinstance(the_date, str):
        try:
            the_datetime = parse(the_date)
            the_date = the_datetime.date()
        except Exception as e:
            payload = {
                ERROR_MESSAGE: str(e)
            }
            return Response(json.dumps(payload), HTTPStatus.INTERNAL_SERVER_ERROR, mimetype=JSON_MIMETYPE)
    try:
        price = get_price(ticker, the_date)
    except Exception as e:
        payload = {
            ERROR_MESSAGE: str(e)
        }
        return Response(json.dumps(payload), HTTPStatus.INTERNAL_SERVER_ERROR, mimetype=JSON_MIMETYPE)
    volume = get_volume()
    stamp = now().strftime(TIMESTAMP_FORMAT)
    payload = {
        "stamp": stamp,
        "date": the_date.strftime(DATE_FORMAT),
        "ticker": ticker,
        "price": price,
        "volume": volume,

    }
    return Response(json.dumps(payload), HTTPStatus.OK, mimetype=JSON_MIMETYPE)


if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", STOCK_SERVICE_PORT)), host="0.0.0.0")