from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import from_json, col

# Define el nuevo esquema basado en la estructura de tu JSON
rutasSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("latitud", FloatType(), False),
    StructField("longitud", FloatType(), False)
])

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rutas") \
    .option("delimiter", ",") \
    .option("startingOffsets", "earliest") \
    .load()

df.printSchema()

# Modifica el esquema para adaptarse a la nueva estructura del JSON
df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), rutasSchema).alias("data")).select("data.*")
df1.printSchema()

df1.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
