from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("ComplianceMonitoring") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()


def process_stream():
    """Process streaming data from Kafka"""
    spark = create_spark_session()

    # Define schema for streaming data
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("sender", StringType(), True),
        StructField("receiver", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("risk_score", DoubleType(), True)
    ])

    # Read streaming data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .load()

    # Parse JSON data
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_value")
    ).select("parsed_value.*")

    # Apply compliance rules
    compliance_df = apply_compliance_rules(parsed_df)

    # Write results to console (for testing)
    query = compliance_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


def apply_compliance_rules(df):
    """Apply compliance rules to the streaming data"""
    return df \
        .withColumn("high_value_flag",
                    when(col("amount") > 500000, True).otherwise(False)) \
        .withColumn("high_risk_flag",
                    when(col("risk_score") > 0.7, True).otherwise(False)) \
        .withColumn("compliance_check_timestamp", current_timestamp()) \
        .withColumn("needs_review",
                    (col("high_value_flag") | col("high_risk_flag")))


if __name__ == "__main__":
    print("Starting Spark Processor...")
    process_stream()