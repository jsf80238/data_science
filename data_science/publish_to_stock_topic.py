import json
import shutil
from datetime import date, timedelta
from pathlib import Path
from random import choice, gauss
from tempfile import NamedTemporaryFile
from time import sleep, time_ns
# Imports above are standard Python
# Imports below are 3rd-party
import redis
import requests
# Imports below are custom
from lib.base import Logger
from lib.stock import STOCK_SERVICE_URL, STOCK_SERVICE_PORT, TICKER_DICT, FILE_SOURCE_DIR

INTERVAL = 1  # seconds
MAX_BATCH_SIZE = 10
REFERENCE_DATE = date.today()
logger = Logger().get_logger()
redis_client = redis.StrictRedis()


def write_batch(
    data: list,
    target_dir: Path=FILE_SOURCE_DIR,
) -> Path:
    """

    :param data: zero or more records
    :param target_dir: where this data will be written
    """
    target = NamedTemporaryFile("w")
    for item in data_list:
        print(str(item), file=target)
    final_target_path = target_dir / str(time_ns())
    shutil.move(target.name, final_target_path)
    return final_target_path


count = 0
data_list = list()
while True:
    count += 1
    ticker = choice(list(TICKER_DICT.keys()))
    date_offset = round(gauss(0, 5))
    the_date = REFERENCE_DATE + timedelta(date_offset)
    URL = f"http://{STOCK_SERVICE_URL}:{STOCK_SERVICE_PORT}/{ticker}/{the_date}"
    response = requests.get(URL)
    if response.status_code != requests.codes.ok:
        logger.error(response.text)
        sleep(5)
        continue
    result = response.json()
    data_list.append(result)
    if count == MAX_BATCH_SIZE:
        target_path = write_batch(
            data=data_list,
            target_dir=FILE_SOURCE_DIR,
        )
        count = 0
        data_list = list()
        logger.info(f"Published {MAX_BATCH_SIZE} records to '{target_path}'.")
    # redis_client.publish(REDIS_CHANNEL_NAME, json.dumps(result))
    # logger.info(f"Published to channel '{REDIS_CHANNEL_NAME}': " + json.dumps(result))
    sleep(1)