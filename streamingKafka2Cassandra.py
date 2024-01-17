from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.sql.functions import from_json, col

# Define el nuevo esquema basado en la estructura de tu JSON
rutasSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("latitud", FloatType(), False),
    StructField("longitud", FloatType(), False)
])

spark = SparkSession \
    .builder \
    .appName("SparkStructuredStreaming") \
    .config("spark.cassandra.connection.host", "172.18.0.5") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
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

# Modifica la lógica para adaptarse a la nueva estructura del esquema
df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), rutasSchema).alias("data")).select("data.*")
df1.printSchema()

def writeToCassandra(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="rutas", keyspace="geo") \

df1.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start() \
    .awaitTermination()

df1.show()
