# Tarea 3 - Procesamiento de datos con Apache Spark

Curso Big Data (UNAD).

Realizado por Maria Fernanda torres Sanchez

## Contenido
- `streaming/`: productor de Kafka y consumidor con Spark Streaming.
- `batch/`: análisis batch con Spark (pendiente).

## Requisitos
- Ubuntu 22.04, Java 17, Python 3.10
- PySpark 3.5.3, Kafka 3.9.2

## Ejecución del tiempo real
1. Iniciar ZooKeeper y Kafka.
2. Crear el topic `sensor_data`.
3. Ejecutar `python streaming/kafka_producer.py`.
4. Ejecutar `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/spark_streaming_consumer.py`.
