from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from datetime import datetime
import json
import tempfile
import subprocess

def main():

    spark = SparkSession.builder \
        .appName("LoadAndPredictApp") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "24") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()
    
    # Load Model
    print("Loading current model...")
    date_str = datetime.now().strftime("%Y-%m-%d")
    model_path = f"s3a://af-weather-lake-01/models/date={date_str}/model/"
    model = PipelineModel.load(model_path)
    
    # Get most recent weather data
    print("Retrieving most recent weather data from S3...")
    current_time = datetime.now().strftime("%Y-%m-%d_%H%M")
    havana_entry = spark.read.json(f"s3a://af-weather-lake-01/data/weather/date={current_time}/havana")
    nassau_entry = spark.read.json(f"s3a://af-weather-lake-01/data/weather/date={current_time}/nassau")
    miami_entry = spark.read.json(f"s3a://af-weather-lake-01/data/weather/date={current_time}/miami")

    print("Performing df operations and making predicitions...")
    # 3-row dataframe
    all_entries = havana_entry.union(nassau_entry).union(miami_entry)

    # add 2 new columns
    df = all_entries.select("current.*", "latitude", "longitude") \
        .withColumn("file_path", input_file_name()) \
        .withColumn("city", regexp_extract("file_path", r"date=[^/]+/([^/]+)/[^/]+\.json$", 1))
    
    # Make Predictions
    predictions = model.transform(df)
    print("Done")

    # Grade accuracy 
    accuracty_evaluator = MulticlassClassificationEvaluator(labelCol='city_index', predictionCol='prediction', metricName='accuracy')
    accuracy = accuracty_evaluator.evaluate(predictions) * 100
    print(f'Accuracy = {accuracy:.2f}%')

    # Grade precision
    precision_evaluator = MulticlassClassificationEvaluator(labelCol='city_index', predictionCol='prediction', metricName='precisionByLabel')
    precision = precision_evaluator.evaluate(predictions) * 100
    print(f'Precision = {precision:.2f}%')

    # Make Report and convert to json
    report = {
        "model_type": "PipelineModel",
        "model_location": f"s3a://af-weather-lake-01/models/date={date_str}/model/",
        "datetime": current_time,
        "accuracy": f'{accuracy:.2f}%',
        "precision": f'{precision:.2f}%',
        "city_indexes": {
            "miami": predictions.select("city_index").where(predictions.city == "miami").first()[0],
            "massau": predictions.select("city_index").where(predictions.city == "nassau").first()[0],
            "havana": predictions.select("city_index").where(predictions.city == "havana").first()[0],
        },
        "city_guesses": {
            "miami": predictions.select("prediction").where(predictions.city == "miami").first()[0],
            "nassau": predictions.select("prediction").where(predictions.city == "nassau").first()[0],
            "havana": predictions.select("prediction").where(predictions.city == "havana").first()[0],
        }
    }

    json_string = json.dumps(report, indent=2)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
        temp_file.write(json_string)
        temp_path = temp_file.name

    try:
        key = f'reports/hourly/date={current_time}/{current_time}_report.json'
        aws_result = subprocess.run([
            'aws', 's3', 'cp', 
            temp_path, 
            f's3://af-weather-lake-01/{key}',
            '--content-type', 'application/json'
        ], capture_output=True, text=True, timeout=60)
        
        if aws_result.returncode == 0:
            print(f"Successfully uploaded to S3: {key}")
        else:
            print(f"AWS CLI failed: {aws_result.stderr}")
            raise Exception("S3 upload failed")
    finally:
        # Clean up temp file
        os.unlink(temp_path)

  

    

if __name__ == "__main__":
    main()