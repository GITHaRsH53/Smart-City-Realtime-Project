from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from config import configuration


def send_sns_alert(incident_row):
    import boto3

    sns = boto3.client('sns', region_name='us-east-1')  # update region as needed
    topic_arn = "arn:aws:sns:us-east-1:730335375154:emergency_alerts"  # Replace with your actual ARN

    message = f"""
    🚨 Emergency Alert 🚨
    Type: {incident_row['type']}
    Location: {incident_row['location']}
    Description: {incident_row['description']}
    Time: {incident_row['timestamp']}
    """

    sns.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject="Smart City Emergency Incident"
    )


def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.rpc.askTimeout", "300s") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Schemas
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    # Read streams
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # Filter active emergency incidents
    activeEmergencies = emergencyDF.filter(col("status") == "active")

    # Write data to S3
    query1 = streamWriter(vehicleDF, 's3a://spark-streaming-datav1/checkpoints/vehicle_data',
                          's3a://spark-streaming-datav1/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://spark-streaming-datav1/checkpoints/gps_data',
                          's3a://spark-streaming-datav1/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://spark-streaming-datav1/checkpoints/traffic_data',
                          's3a://spark-streaming-datav1/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://spark-streaming-datav1/checkpoints/weather_data',
                          's3a://spark-streaming-datav1/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://spark-streaming-datav1/checkpoints/emergency_data',
                          's3a://spark-streaming-datav1/data/emergency_data')

    # Send SNS alerts for active emergencies
    def notify_active_emergencies(batch_df, batch_id):
        emergencies = batch_df.collect()
        for row in emergencies:
            send_sns_alert(row.asDict())

    notification_query = (activeEmergencies.writeStream
                          .foreachBatch(notify_active_emergencies)
                          .outputMode("update")
                          .start())

    # Await all
    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
    query5.awaitTermination()
    notification_query.awaitTermination()


if __name__ == "__main__":
    main()
