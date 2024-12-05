# src/data_processing/transformations.py
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg, max, min, count

class TaskLogTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transform_logs(self, df: DataFrame) -> DataFrame:
        """
        Perform comprehensive transformations on task logs:
        1. Aggregate hours by project
        2. Calculate user productivity
        3. Identify long-running tasks
        
        Args:
            df (DataFrame): Input task log DataFrame
        
        Returns:
            DataFrame: Transformed and enriched dataset
        """
        try:
            # Project-level aggregations
            project_summary = (df
                .groupBy("project")
                .agg(
                    sum("duration").alias("total_hours"),
                    avg("duration").alias("avg_task_duration"),
                    count("task_id").alias("total_tasks"),
                    max("duration").alias("max_task_duration")
                ))

            # User productivity analysis
            user_productivity = (df
                .groupBy("user")
                .agg(
                    sum("duration").alias("total_hours"),
                    avg("duration").alias("avg_task_duration"),
                    count("task_id").alias("total_tasks")
                ))

            # Identify long-running tasks (more than 8 hours)
            long_running_tasks = df.filter(col("duration") > 8)

            self.logger.info("Completed log transformations successfully")
            return project_summary.join(user_productivity, "user")

        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            raise