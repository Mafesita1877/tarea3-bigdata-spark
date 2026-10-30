from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    window
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType
)

spark = (
    SparkSession.builder
    .appName("AnalisisSensoresKafka")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

datos = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn(
        "fecha_evento",
        col("timestamp").cast("timestamp")
    )
    .filter(col("fecha_evento").isNotNull())
)

estadisticas = (
    datos
    .groupBy(
        window(col("fecha_evento"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        count("*").alias("eventos"),
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio")
    )
)

query = (
    estadisticas.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "50")
    .option(
        "checkpointLocation",
        "checkpoints/sensores_v1"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
