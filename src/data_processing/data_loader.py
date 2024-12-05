# src/data_processing/data_loader.py
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

class TaskLogDataLoader:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)

    def _define_schema(self) -> StructType:
        """
        Define a robust schema for task log data.
        
        Schema includes:
        - task_id: Unique identifier for each task
        - project: Project name
        - user: User who performed the task
        - start_time: Task start timestamp
        - end_time: Task end timestamp
        - duration: Task duration in hours
        """
        return StructType([
            StructField("task_id", StringType(), False),
            StructField("project", StringType(), False),
            StructField("user", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("duration", DoubleType(), False)
        ])

    def load_task_logs(self, data_path: str) -> DataFrame:
        """
        Load task logs with schema validation and error handling.
        
        Args:
            data_path (str): Path to the task log CSV/Parquet file
        
        Returns:
            DataFrame: Validated and loaded task logs
        """
        try:
            # Attempt to load data with predefined schema
            df = (self.spark.read
                  .format("csv")  # or "parquet" depending on source
                  .option("header", "true")
                  .option("mode", "DROPMALFORMED")  # Drop malformed records
                  .schema(self._define_schema())
                  .load(data_path))

            # Basic validation
            record_count = df.count()
            self.logger.info(f"Loaded {record_count} task log records")

            if record_count == 0:
                raise ValueError("No valid records found in the dataset")

            return df

        except Exception as e:
            self.logger.error(f"Error loading task logs: {e}")
            raise