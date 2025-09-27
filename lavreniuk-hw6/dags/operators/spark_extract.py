import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, hour, unix_timestamp
from pyspark.sql.functions import round as sp_round
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

FILES_PATH = "/opt/spark/unified_data/"

GREEN_TAXI_DIR_PATH = os.path.join(FILES_PATH, "green_taxi")
YELLOW_TAXI_DIR_PATH = os.path.join(FILES_PATH, "yellow_taxi")
TAXI_ZONE_FILE_PATH = os.path.join(FILES_PATH, "taxi_zone_lookup.csv")

DB_USER = "airflow"
DB_PASSWORD = "airflow"
JDBC_URL = "jdbc:postgresql://host.docker.internal:8432/hw6"


def to_snake_case(name: str) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def join_zones(df: DataFrame, zone_df: DataFrame) -> DataFrame:
    return df.join(
        broadcast(zone_df.select(col("Borough").alias("pickup_borough"), col("LocationID").alias("pu_location_id"))),
        how="left",
        on="pu_location_id",
    ).join(
        broadcast(zone_df.select(col("Borough").alias("dropoff_borough"), col("LocationID").alias("do_location_id"))),
        how="left",
        on="do_location_id",
    )


def write_psql(
    writeable_df: DataFrame, *, table: str, connection_str: str, username: str, password: str, mode: str = "overwrite"
) -> None:
    properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}
    writeable_df.write.jdbc(url=connection_str, table=table, properties=properties, mode=mode)


spark = (
    SparkSession.builder.appName("lavreniuk-hw6")
    .config("spark.jars", "/opt/spark/artifacts/postgresql-42.7.7.jar")
    # .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

# Read previously prepared data (data_unification.ipynb)
common_schema = StructType(
    [
        StructField("VendorID", LongType(), True),
        StructField("PULocationID", LongType(), True),  # green
        StructField("trip_distance", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),  # Use Double here
    ]
)

yellow_schema = StructType(
    [
        StructField("RatecodeID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("trip_type", DoubleType(), True),
        *common_schema,
    ]
)

green_schema = StructType(
    [
        StructField("RatecodeID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("ehail_fee", DoubleType(), True),
        *common_schema,
    ]
)

green_df = (
    spark.read.options(mergeSchema="true", recursiveFileLookup="true")
    .schema(green_schema)
    .parquet(GREEN_TAXI_DIR_PATH)
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("ehail_fee", "fee")
)

yellow_df = (
    spark.read.options(mergeSchema="true", recursiveFileLookup="true")
    .schema(yellow_schema)
    .parquet(YELLOW_TAXI_DIR_PATH)
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
)

# * Видалення поїздок з нульовою або від’ємною відстанню (trip_distance <= 0).
# * Відсіювання аномальних поїздок з тарифом ≤ 0 або > 500.
# * Відсіювання записів з некоректною кількістю пасажирів (0 або більше 6).

filter_query = """
    SELECT * FROM {df}
    WHERE 
        trip_distance > 0
        AND total_amount > 0
        AND total_amount <= 500
        AND passenger_count BETWEEN 0 AND 6;   
"""
yellow_filtered_df = spark.sql(filter_query, df=yellow_df)
green_filtered_df = spark.sql(filter_query, df=green_df)

# * Уніфікація назв колонок до snake_case.
# * Конвертація дат у формат timestamp. - Already done
# * Обчислення додаткових полів:
#   * Тривалість поїздки у хвилинах (trip_duration_minutes).
#   * Година доби початку поїздки (pickup_hour).


additional_columns = {
    "pickup_hour": hour("pickup_datetime"),
    "trip_duration_minutes": sp_round(
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60, 4
    ),
}

yellow_snaked_df = yellow_filtered_df.toDF(*[to_snake_case(c) for c in yellow_filtered_df.columns]).withColumns(
    additional_columns
)

green_snaked_df = green_filtered_df.toDF(*[to_snake_case(c) for c in green_filtered_df.columns]).withColumns(
    additional_columns
)

# * Об’єднання з довідником зон за PULocationID та DOLocationID:
# * Отримання назв районів (наприклад, pickup_borough, dropoff_borough).

taxi_zone_lookup_df = spark.read.options(header="true").csv(TAXI_ZONE_FILE_PATH)


yellow_taxi_stage_df = join_zones(yellow_snaked_df, taxi_zone_lookup_df)
green_taxi_stage_df = join_zones(green_snaked_df, taxi_zone_lookup_df)

# * Основний датафрейм записується у таблицю yellow_taxi_stage, green_taxi_stage, taxi_lookup_zone_stage
# * Формат завантаження: overwrite, залежно від налаштувань.

write_psql(
    yellow_taxi_stage_df.limit(20000),
    table="yellow_taxi_stage",
    connection_str=JDBC_URL,
    username=DB_USER,
    password=DB_PASSWORD,
)
write_psql(
    green_taxi_stage_df.limit(20000),  # Use 20000 because I catch OOM Error
    table="green_taxi_stage",
    connection_str=JDBC_URL,
    username=DB_USER,
    password=DB_PASSWORD,
)
write_psql(
    taxi_zone_lookup_df.toDF(*[to_snake_case(c) for c in taxi_zone_lookup_df.columns]),
    table="taxi_lookup_zone_stage",
    connection_str=JDBC_URL,
    username=DB_USER,
    password=DB_PASSWORD,
)
