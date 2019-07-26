from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
sqlContext = SQLContext(sc)

customSchema = StructType([ \
    StructField("vehicle", StringType(), True), \
    StructField("driver", StringType(), True), \
    StructField("pickup_datetime_s", StringType(), True), \
    StructField("dropoff_datetime_s", StringType(), True), \
    StructField("trip_time_in_secs", IntegerType(), True), \
    StructField("trip_distance", FloatType(), True), \
    StructField("pickup_longitude", FloatType(), True), \
    StructField("pickup_latitude", FloatType(), True), \
    StructField("dropoff_longitude", FloatType(), True), \
    StructField("dropoff_latitude", FloatType(), True), \
    StructField("payment_type", StringType(), True), \
    StructField("fare_amount", FloatType(), True), \
    StructField("surcharge", FloatType(), True), \
    StructField("mta_tax", FloatType(), True), \
    StructField("tip_amount", FloatType(), True), \
    StructField("tolls_amount", FloatType(), True), \
    StructField("total_amount", FloatType(), True)])

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('file:///Users/dani/Temp/sorted_data.csv', schema = customSchema)
df.registerTempTable('trip')