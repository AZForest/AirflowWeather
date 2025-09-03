from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
import argparse
import os
import psycopg2

def main():
    print("Hello World")
    print(os.getenv("AURORA_USER"))
    print(os.getenv("AURORA_TOKEN"))
    # AURORA_HOST = os.getenv("AURORA_HOST")
    # AURORA_USER = os.getenv("AURORA_USER")
    # AURORA_TOKEN = os.getenv("AURORA_TOKEN")
    # AURORA_PORT = int(os.getenv("AURORA_PORT", "5432"))
    # DB_NAME = os.getenv("AURORA_DB", "postgres")
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host")
    parser.add_argument("--db-port")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password-file")
    args = parser.parse_args()

    with open(args.db_password_file, "r") as f:
        db_password = f.read().strip()
        
    print(db_password)

    spark = SparkSession.builder \
        .appName("S3ToAuroraPostgres") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "24") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()

    df = spark.read.json("s3a://af-weather-lake-01/data/weather/")

    new_df = df.select("current.*", "date", "latitude", "longitude") \
    .withColumn("file_path", input_file_name()) \
    .withColumn("city", regexp_extract("file_path", r"date=[^/]+/([^/]+)/[^/]+\.json$", 1))

    # Aurora PostgreSQL connection details
    # aurora_jdbc_url = "jdbc:postgresql://kaabullyzugwom3tv55kdlehui.dsql.us-west-2.on.aws:5432/postgres"
    # aurora_properties = {
    #     "user": os.getenv("AURORA_USER"),
    #     "password": os.getenv("AURORA_TOKEN"),
    #     "driver": "org.postgresql.Driver",
    #     "autocommit": "true" 
    # }

    # new_df.write \
    #     .mode("append") \
    #     .jdbc(url=aurora_jdbc_url, table="weather_data", properties=aurora_properties)
        
    
    # print("Data written to Aurora PostgreSQL")

    # Function to insert a partition of rows into Aurora
    def write_partition(rows):
        # conn = psycopg2.connect(
        #     host=AURORA_HOST,
        #     dbname="postgres",
        #     user=AURORA_USER,
        #     password=AURORA_TOKEN,
        #     port=AURORA_PORT,
        #     sslmode='require'  # Aurora generally requires SSL
        # )
        conn = psycopg2.connect(
            host=args.db_host,
            dbname="postgres",
            user=args.db_user,
            password=db_password,
            port=args.db_port,
            sslmode='require'  # Aurora generally requires SSL
        )
        cur = conn.cursor()
        insert_query = """
            INSERT INTO weather_data (
                apparent_temperature, cloud_cover, "interval", is_day, precipitation,
                pressure_msl, rain, relative_humidity_2m, showers, surface_pressure,
                temperature_2m, "time", weather_code, wind_direction_10m, wind_gusts_10m,
                wind_speed_10m, latitude, longitude, file_path, city, "date"
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (date, city) DO NOTHING
        """
        for row in rows:
            cur.execute(insert_query, (
                row.apparent_temperature,
                row.cloud_cover,
                row.interval,
                row.is_day,
                row.precipitation,
                row.pressure_msl,
                row.rain,
                row.relative_humidity_2m,
                row.showers,
                row.surface_pressure,
                row.temperature_2m,
                row.time,
                row.weather_code,
                row.wind_direction_10m,
                row.wind_gusts_10m,
                row.wind_speed_10m,
                row.latitude,
                row.longitude,
                row.file_path,
                row.city,
                row.date
            ))
        conn.commit()
        cur.close()
        conn.close()

    # Write to Aurora using foreachPartition
    new_df.foreachPartition(write_partition)
    print("Data written to Aurora PostgreSQL successfully")

    spark.stop()

if __name__ == "__main__":
    main()

# apparent_temperature|
# cloud_cover|
# interval|
# is_day|
# precipitation|
# pressure_msl|
# rain|
# relative_humidity_2m|
# showers|
# surface_pressure|
# temperature_2m|
# time
# |weather_code|
# wind_direction_10m|
# wind_gusts_10m|
# wind_speed_10m|
# date|
# latitude|
# longitude|
# file_path|
# city|