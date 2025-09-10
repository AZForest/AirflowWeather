from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

from datetime import datetime
import json
import tempfile
import subprocess
import os

def main():

    spark = SparkSession.builder \
        .appName("S3ToAuroraPostgres") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "24") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()
    
    # Get and clean data
    df = spark.read.json("s3a://af-weather-lake-01/data/weather/")

    work_df = df.select("current.*", "date", "latitude", "longitude") \
        .withColumn("file_path", input_file_name()) \
        .withColumn("city", regexp_extract("file_path", r"date=[^/]+/([^/]+)/[^/]+\.json$", 1))
    
    valid_dates_df = work_df.groupBy("date") \
        .agg(
            F.count("*").alias("num_of_dates")
        ) \
        .where("num_of_dates == 3") \
        .select("date") 
    clean_df = work_df.join(valid_dates_df, "date", "inner")
    num_rows = clean_df.count()
    # Build model
    pipeline_stages = []

    #String Indexer
    indexer = StringIndexer(inputCol="city", outputCol="city_index")
    pipeline_stages.append(indexer)

    #List Columns for VectorAssembler
    input_columns = ["apparent_temperature", "cloud_cover", "interval",
                 "precipitation", "pressure_msl",
                "rain", "relative_humidity_2m", "showers",
                 "temperature_2m",
                  "wind_direction_10m", "wind_gusts_10m", "wind_speed_10m"
                 ]
    
    # Vector Assembler
    assembler = VectorAssembler(inputCols=input_columns, outputCol="unscaled_features")
    pipeline_stages.append(assembler)

    # Standard Scaler
    scaler = StandardScaler(inputCol='unscaled_features', outputCol='features', withMean=True, withStd=True)
    pipeline_stages.append(scaler)

    # Decision Tree Classifier
    dtc = DecisionTreeClassifier(featuresCol='features', labelCol='city_index')
    pipeline_stages.append(dtc)

    # Build Pipeline
    pipeline = Pipeline(stages=pipeline_stages)

    # Build Model/Train
    model = pipeline.fit(clean_df)

    # Save model
    date_str = datetime.now().strftime("%Y-%m-%d")
    model_path = f"s3a://af-weather-lake-01/models/date={date_str}/model/"
    # model.write().overwrite().save(model_path)

    # Save custom model specific metadata
    custom_metadata_path = f"s3a://af-weather-lake-01/models/date={date_str}/custom_model_metadata/"
    metadata = {
        "input_columns": input_columns,
        "training_split_used": "None",
        "training_entries_count": num_rows,
        "paramGridBuilderValues": None,
    }

    json_string = json.dumps(metadata, indent=2)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
        temp_file.write(json_string)
        temp_path = temp_file.name

    try:
        aws_result = subprocess.run([
            'aws', 's3', 'cp', 
            temp_path, 
            f"{custom_metadata_path}/custom_model_metadata.json"
            '--content-type', 'application/json'
        ], capture_output=True, text=True, timeout=60)
        
        if aws_result.returncode == 0:
            print(f"Successfully uploaded to S3: {custom_metadata_path}")
        else:
            print(f"AWS CLI failed: {aws_result.stderr}")
            raise Exception("S3 upload failed")
    finally:
        # Clean up temp file
        os.unlink(temp_path)



    
    

if __name__ == "__name__":
    main()