import logging
import sys

def setup_logging(log_level=logging.INFO):
    """
    Configure comprehensive logging for the application.
    
    Args:
        log_level (int): Logging level
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('task_log_analysis.log')
        ]
    )