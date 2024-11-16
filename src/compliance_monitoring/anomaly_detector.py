from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


class AnomalyDetector:
    def __init__(self, spark):
        self.spark = spark
        self.model = None

    def train(self, training_data):
        """Train anomaly detection model"""
        assembler = VectorAssembler(
            inputCols=["amount", "risk_score"],
            outputCol="features"
        )

        # Train KMeans model
        kmeans = KMeans(k=3, seed=1)
        self.model = kmeans.fit(assembler.transform(training_data))

    def detect_anomalies(self, streaming_data):
        """Detect anomalies in streaming data"""
        assembler = VectorAssembler(
            inputCols=["amount", "risk_score"],
            outputCol="features"
        )

        transformed_data = self.model.transform(
            assembler.transform(streaming_data)
        )

        return transformed_data.withColumn(
            "is_anomaly",
            transformed_data.prediction > 1
        )