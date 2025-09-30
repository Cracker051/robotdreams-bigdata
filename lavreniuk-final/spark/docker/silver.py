import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, length, lit, to_timestamp, trim, when
from pyspark.sql.types import BooleanType, DoubleType, IntegerType


FILES_PATH = "/opt/spark/data/bronze"

ARREST_PATH = os.path.join(FILES_PATH, "arrests")
CRIME_PATH = os.path.join(FILES_PATH, "crimes")
CODE_PATH = os.path.join(FILES_PATH, "iucr")

DB_USER = "airflow"
DB_PASSWORD = "airflow"
JDBC_URL = "jdbc:postgresql://host.docker.internal:8432/final"


def to_snake_case(name: str) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.replace(" ", "_").lower()


def write_psql(
    writeable_df: DataFrame, *, table: str, connection_str: str, username: str, password: str, mode: str = "overwrite"
) -> None:
    properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}
    writeable_df.write.jdbc(url=connection_str, table=table, properties=properties, mode=mode)


spark = (
    SparkSession.builder.appName("silver-preparation")
    .config("spark.jars", "./artifacts/postgresql-42.7.7.jar")
    .getOrCreate()
)

crimes_df = spark.read.options(recursiveFileLookup="true").parquet(CRIME_PATH)
arrest_df = spark.read.options(recursiveFileLookup="true").parquet(ARREST_PATH)
code_df = spark.read.parquet(CODE_PATH)


crimes_silver_df = (
    crimes_df.withColumn("crime_id", col("ID"))
    .withColumn("case_number", col("Case Number"))
    .withColumn("crime_datetime", col("Date"))
    .withColumn("block", col("Block"))
    .withColumn("iucr_code", col("IUCR"))
    .withColumn("primary_type", col("Primary Type"))
    .withColumn("description", col("Description"))
    .withColumn("location_description", col("Location Description"))
    .withColumn("is_arrest", col("Arrest"))
    .withColumn("is_domestic", col("Domestic"))
    .withColumn("beat_id", col("Beat"))
    .withColumn("district_id", col("District"))
    .withColumn("ward_id", col("Ward"))
    .withColumn("community_area_id", col("Community Area"))
    .withColumn("fbi_code", col("FBI Code"))
    .withColumn("x_coordinate", col("X Coordinate"))
    .withColumn("y_coordinate", col("Y Coordinate"))
    .withColumn("latitude", col("Latitude"))
    .withColumn("longitude", col("Longitude"))
    .withColumn("location_geo_point", col("Location"))
    .select(
        "crime_id",
        "case_number",
        "crime_datetime",
        "block",
        "iucr_code",
        "primary_type",
        "description",
        "location_description",
        "is_arrest",
        "is_domestic",
        "beat_id",
        "ward_id",
        "community_area_id",
        "fbi_code",
        "x_coordinate",
        "y_coordinate",
        "latitude",
        "longitude",
        "location_geo_point",
        "district_id",
    )
    .withColumns(
        {
            "crime_datetime": to_timestamp(col("crime_datetime"), "MM/dd/yyyy hh:mm:ss a"),
            "is_arrest": col("is_arrest").cast(BooleanType()),
            "is_domestic": col("is_domestic").cast(BooleanType()),
            "latitude": col("latitude").cast(DoubleType()),
            "longitude": col("longitude").cast(DoubleType()),
            "x_coordinate": col("x_coordinate").cast(DoubleType()),
            "y_coordinate": col("y_coordinate").cast(DoubleType()),
            "beat_id": col("beat_id").cast(IntegerType()),
            "district_id": col("district_id").cast(IntegerType()),
            "ward_id": col("ward_id").cast(IntegerType()),
            "community_area_id": col("community_area_id").cast(IntegerType()),
            ## Transform section
            "location_description": coalesce(col("location_description"), lit("Unknown")),
        }
    )
    .filter(
        col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("iucr_code").isNotNull()
        & col("primary_type").isNotNull()
    )
    .dropDuplicates(["crime_id"])
)

arrests_renamed_df = (
    arrest_df.withColumn("arrest_id", col("CB_NO"))
    .withColumn("arrestee_race", col("RACE"))
    .withColumn("charge_one_statue", col("CHARGE 1 STATUTE"))
    .withColumn("charge_one_description", col("CHARGE 1 DESCRIPTION"))
    .withColumn("charge_one_type", col("CHARGE 1 TYPE"))
)

arrests_silver_df = (
    arrests_renamed_df.toDF(*[to_snake_case(c) for c in arrests_renamed_df.columns])
    .select(
        "arrest_id",
        "case_number",
        "arrest_date",
        "arrestee_race",
        "charge_one_statue",
        "charge_one_description",
        "charge_one_type",
    )
    .withColumns(
        {
            "arrest_date": to_timestamp(col("arrest_date"), "MM/dd/yyyy hh:mm:ss a"),
            "charge_one_type": (
                when(col("charge_one_type") == "F", "felony")
                .when(col("charge_one_type") == "M", "misdemeanor")
                .otherwise("other")
            ),
        }
    )
    .filter((col("case_number").isNotNull()) & (length(trim(col("case_number"))) > 0))
    .dropDuplicates(["arrest_id"])
)

codes_renamed_df = code_df.withColumnRenamed("IUCR", "iucr_code")

codes_silver_df = (
    codes_renamed_df.toDF(*[to_snake_case(c) for c in codes_renamed_df.columns])
    .withColumns(
        {
            "primary_description": coalesce(col("primary_description"), lit("Unknown")),
            "secondary_description": coalesce(col("secondary_description"), lit("Unknown")),
        }
    )
    .filter(  # Dont parse all columns to string, because here are boolean
        col("active").cast("boolean")
    )
    .dropDuplicates(["iucr_code"])
    .select("iucr_code", "primary_description", "secondary_description", "index_code")
)


write_psql(codes_silver_df, table="silver_ucr_codes", connection_str=JDBC_URL, username=DB_USER, password=DB_PASSWORD)
write_psql(
    arrests_silver_df,
    table="silver_arrests",
    connection_str=JDBC_URL,
    username=DB_USER,
    password=DB_PASSWORD,
    mode="append",
)  # pyspark doesnt have mode merge
write_psql(
    crimes_silver_df,
    table="silver_crimes",
    connection_str=JDBC_URL,
    username=DB_USER,
    password=DB_PASSWORD,
    mode="append",
)  #  pyspark doesnt have mode merge
