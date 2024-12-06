import sys
import logging
import os
from pyspark.sql import SparkSession
from utils.config import load_config
from utils.logging_config import setup_logging
from data_processing.data_loader import TaskLogDataLoader
from data_processing.transformations import TaskLogTransformer
from data_processing.analytics import TaskLogAnalytics
from data_processing.data_generator import TaskLogGenerator

def generate_data(config):
    """
    Generate task log data if not already present
    
    Args:
        config (dict): Configuration dictionary
    """
    generator = TaskLogGenerator(config)
    
    # Check if data already exists
    if not os.path.exists(config['data']['input_path']):
        logging.info("Generating task log data...")
        task_logs = generator.generate_task_logs()
        generator.save_to_parquet(task_logs)
    else:
        logging.info("Task log data already exists. Skipping generation.")

def main():
    # Initialize logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = load_config()

        # Generate data if needed
        generate_data(config)

        # Initialize Spark Session with optimized configurations
        spark = (SparkSession.builder
            .appName(config['spark']['app_name'])
            .config("spark.sql.shuffle.partitions", 200)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.dynamicAllocation.enabled", "true")
            .getOrCreate())

        # Set log level
        spark.sparkContext.setLogLevel("WARN")

        # Load data with robust error handling
        data_loader = TaskLogDataLoader(spark)
        task_logs = data_loader.load_task_logs(config['data']['input_path'])

        # Perform transformations
        transformer = TaskLogTransformer()
        processed_logs = transformer.transform_logs(task_logs)

        combined_summary, _, project_summary = processed_logs  # Unpack the tuple

        project_summary.show()


        # Perform analytics
        analytics = TaskLogAnalytics()
        results = analytics.analyze_task_logs(project_summary)

        # Write results
        results.write.mode("overwrite").parquet(config['data']['output_path'])

        logger.info("Task log analysis completed successfully!")

    except Exception as e:
        logger.error(f"Critical error in task log processing: {e}")
        sys.exit(1)
    finally:
        # Ensure Spark session is closed
        spark.stop()

if __name__ == "__main__":
    main()