from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Инициализация Spark
spark = SparkSession.builder \
    .appName("TelegramStreamingAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SPARK STREAMING CONSUMER ИНИЦИАЛИЗИРОВАН")
print("=" * 80)

# Схема данных из Kafka
schema = StructType([
    StructField("username", StringType()),
    StructField("timestamp", StringType()),
    StructField("channel_id", LongType()),
    StructField("message_text", StringType())
])

try:
    # Читаем из Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "telegram_data") \
        .option("startingOffsets", "latest") \
        .load()

    # Парсим JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Преобразуем timestamp
    parsed_df = parsed_df.withColumn("timestamp_ts", col("timestamp").cast("timestamp"))

    # ЗАДАНИЕ 1: Подсчёт сообщений за 1 минуту
    window_1min = parsed_df \
        .groupBy(
            window(col("timestamp_ts"), "1 minute", "30 seconds"),
            "username"
        ) \
        .agg(count("*").alias("message_count")) \
        .select(
            col("window.start").alias("start"),
            col("window.end").alias("end"),
            "username",
            "message_count"
        )

    # ЗАДАНИЕ 1: Подсчёт сообщений за 10 минут
    window_10min = parsed_df \
        .groupBy(
            window(col("timestamp_ts"), "10 minute", "30 seconds"),
            "username"
        ) \
        .agg(count("*").alias("message_count")) \
        .select(
            col("window.start").alias("start"),
            col("window.end").alias("end"),
            "username",
            "message_count"
        )

    print("\n" + "=" * 80)
    print("📊 ЗАДАНИЕ 1: Подсчёт сообщений (1 и 10 минут)")
    print("=" * 80 + "\n")

    # Вывод для 1 минуты (БЕЗ orderBy в Update mode!)
    query_1min = window_1min \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 50) \
        .start()

    # Вывод для 10 минут (БЕЗ orderBy в Update mode!)
    query_10min = window_10min \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 50) \
        .start()

    print("\n🎯 НАЧАЛО ОБРАБОТКИ ПОТОКА ДАННЫХ\n")

    # Жди
    spark.streams.awaitAnyTermination()

except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()


