# Imports above are standard Python
# Imports below are 3rd-party
import redis
# Imports below are custom
from lib.stock import STOCK_SERVICE_URL, STOCK_SERVICE_PORT

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("stock_price_analysis") \
    .getOrCreate()

userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .json("/path/to/directory")  # Equivalent to format("csv").load("/path/to/directory")