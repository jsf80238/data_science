# Imports above are standard Python
# Imports below are 3rd-party
import redis
# Imports below are custom
from lib.stock import STOCK_SERVICE_URL, STOCK_SERVICE_PORT

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, TimestampType

spark = SparkSession \
    .builder \
    .appName("stock_price_analysis") \
    .getOrCreate()

schema = (
    StructType()
    .add("stamp", StringType())  # Will later convert to TimestampType
    .add("date", StringType())  # Will later convert to DateType
    .add("ticker", StringType())
    .add("price", FloatType())
    .add("volume", IntegerType())
)

data = [('2024-05-09T16:39:59-0600', '2024-04-28', 'NNE', 5.1, 127)]
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

exit()

df = spark \
    .readStream \
    .json("/tmp/stock_data")