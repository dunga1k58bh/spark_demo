from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

#First van can sparksession
spark:SparkSession = SparkSession.builder \
      .appName("Spark streaming test") \
      .getOrCreate()

#O day chi don gian la tao mot streaming bang cach doc cac ban ghi json trong thu muc streaming data

#schema define - neu doc file json thi bat buoc phai co
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)

df_with_schema = spark.readStream \
      .format("json") \
      .option("header", True) \
      .option("host", "master") \
      .option("port", 9999) \
      .option("rowsPerSecond", 1) \
      .schema(schema) \
      .load("file:///home/iloveu/BK_bat_diet/20212/BigData/spark_test/streming_data")

#Transformation lay ra 2 cot zipcode va city sau do loc lay cac hang co zipcode > 10000
zipCode_df = df_with_schema \
      .select("Zipcode", "City") \
      .where("Zipcode > 10000")

query = zipCode_df \
    .writeStream \
    .format("json") \
    .outputMode("append") \
    .option("checkpointLocation", "tmp/checkpoint")\
    .start("output")

query.awaitTermination()

