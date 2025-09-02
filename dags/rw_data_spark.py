from pyspark.sql import SparkSession
import argparse

def main():
    print("Hello World")
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", required=True, help="Path to S3 bucket with the files")
    parser.add_argument("--output_path", required=True, help="Output path for the aggregated results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ReadJsonData").getOrCreate()

    df = spark.read.json("s3://af-weather-lake-0/data/weather/*/")
    df.select("current.temperature_2m", "current.time").show()

    spark.stop()

if __name__ == "__main__":
    main()