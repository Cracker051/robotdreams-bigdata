import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


FILES_PATH = "/opt/spark/data/raw"

ARREST_PATH = os.path.join(FILES_PATH, "arrests")
CRIME_PATH = os.path.join(FILES_PATH, "crimes")
CODE_PATH = os.path.join(FILES_PATH, "iucr")


def listdir_abs(dir_path: str) -> list[str]:
    return [os.path.join(dir_path, filename) for filename in os.listdir(dir_path)]


spark = SparkSession.builder.appName("silver-preparation").getOrCreate()

crimes_df = spark.read.options(header="true", inferSchema="true").csv(path=listdir_abs(CRIME_PATH))
arrest_df = spark.read.options(header="true").csv(path=listdir_abs(ARREST_PATH))
code_df = spark.read.options(header="true").csv(path=listdir_abs(CODE_PATH))


timestamp_column = {"load_timestamp": current_timestamp()}
file_name_column = {"source_file": input_file_name()}


crimes_bronze_df = crimes_df.withColumns({**timestamp_column, **file_name_column})
arrest_bronze_df = arrest_df.withColumns({**timestamp_column, **file_name_column})
code_bronze_df = code_df.withColumns(timestamp_column)

crimes_bronze_df.repartition("source_file").write.format("parquet").mode("append").save("/opt/spark/data/bronze/crimes")
arrest_bronze_df.repartition("source_file").write.format("parquet").mode("append").save(
    "/opt/spark/data/bronze/arrests"
)
code_bronze_df.write.format("parquet").mode("overwrite").save("/opt/spark/data/bronze/iucr")
