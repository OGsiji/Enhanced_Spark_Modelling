from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg, max, min, count, when
import logging

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
                    sum("duration").alias("total_user_hours"),
                    avg("duration").alias("avg_user_task_duration"),
                    count("task_id").alias("user_task_count")
                ))


            # Identify long-running tasks (more than 8 hours)
            long_running_tasks = df.filter(col("duration") > 8)

            self.logger.info("Completed log transformations successfully")
            return  long_running_tasks, project_summary, user_productivity

        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            raise