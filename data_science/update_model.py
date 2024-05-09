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

spark.conf.set("spark.sql.streaming.schemaInference", True)

# Create the streaming_df to read from input directory
df = spark.read \
    .format("json") \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "/tmp") \
    .option("maxFilesPerTrigger", 1) \
    .load("/tmp/stock_data/")

df.show()
