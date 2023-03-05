from prefect import task, flow
import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
from pathlib import Path


@task
def get_data(url, filename: str, color: str, year: str) -> str | None:
    dest = f"data/raw/{color}/{year}"
    file_path = f"{dest}/{filename}"

    if not os.path.exists(dest):
        print(f"Creating folder {dest}")
        os.makedirs(dest)

    if not Path(file_path).is_file():
        print(f"Downloading {filename}")
        result = requests.get(url)
        if result.status_code == 200:
            open(file_path, "wb").write(result.content)
        else:
            print(
                f"Error processing {filename}: code: {result.status_code} message: {result.content}"
            )
            return None

    return file_path


@task
def create_spark_session():
    return SparkSession.builder.appName("default").master("local[*]").getOrCreate()


@task
def write_parquet(
    spark: SparkSession, fp: str, schema: StructType, color: str, year: str, month: int
):
    dest_path = f"data/pq/{color}/{year}/{month:02}"
    spark_df = spark.read.option("header", "true").schema(schema).csv(fp)
    if not Path(dest_path).is_dir():
        spark_df.repartition(8).write.parquet(dest_path)


@flow
def taxi_data_pipeline(
    colors: list[str], years: list[str], month_map: dict, schema_map: dict
):

    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

    spark = create_spark_session()

    for color in colors:
        for year in years:
            months = month_map.get(year, [])
            for month in months:
                filename = f"{color}_tripdata_{year}-{month:02}.csv.gz"
                url = f"{base_url}/{color}/{filename}"
                fp = get_data(url, filename, color, year)
                if fp:
                    schema = schema_map.get(color, None)
                    if schema:
                        write_parquet(spark, fp, schema, color, year, month)
                    else:
                        print(
                            f"Skipping writing parquet for {filename}, no schema found"
                        )
                else:
                    raise Exception(f"{url} not found")


@flow
def fvhv_data_pipeline():
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz"

    spark = create_spark_session()

    result = requests.get(base_url)

    dest_path = "data/raw/fvhv/2021"

    if not Path(dest_path).is_dir():
        os.makedirs(dest_path)

    dest_file_path = f"{dest_path}/fvhv_tripdata_2021-06.csv.gz"

    if not Path(dest_file_path).is_file():
        open(dest_file_path, "wb").write(result.content)

    schema = StructType(
        [
            StructField("dispatching_base_num", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("SR_Flag", StringType(), True),
            StructField("Affiliated_base_number", StringType(), True),
        ]
    )

    spark_df = spark.read.option("header", "true").schema(schema).csv(dest_file_path)
    parquet_fp = "data/pq/fvhv/2021"
    spark_df.repartition(12).write.parquet(parquet_fp, mode="overwrite")


if __name__ == "__main__":
    fvhv_data_pipeline()
