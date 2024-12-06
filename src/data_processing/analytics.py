# src/data_processing/analytics.py
import logging
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression

class TaskLogAnalytics:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def analyze_task_logs(self, transformed_df: DataFrame) -> DataFrame:
        """
        Perform advanced analytics:
        1. Cluster projects based on productivity
        2. Predict task duration
        
        Args:
            transformed_df (DataFrame): Transformed task log data
        
        Returns:
            DataFrame: Analytics results
        """
        try:
            
            transformed_df.show(5)

            # Prepare features for clustering
            assembler = VectorAssembler(
                inputCols=["total_hours", "avg_task_duration", "total_tasks"],
                outputCol="features"
            )
            
            # K-means clustering for project categorization
            kmeans = KMeans(k=3, featuresCol="features", predictionCol="project_cluster")
            cluster_model = kmeans.fit(assembler.transform(transformed_df))
            
            # Linear Regression for task duration prediction
            lr_assembler = VectorAssembler(
                inputCols=["total_hours", "total_tasks"],
                outputCol="lr_features"
            )
            
            lr = LinearRegression(featuresCol="lr_features", labelCol="avg_task_duration")
            lr_model = lr.fit(lr_assembler.transform(transformed_df))

            self.logger.info("Completed advanced analytics")
            transformed_df.show()

            return transformed_df

        except Exception as e:
            self.logger.error(f"Analytics processing error: {e}")
            raise