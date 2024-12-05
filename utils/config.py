import yaml
import logging

def load_config(config_path='config.yaml'):
    """
    Load configuration from YAML file with robust error handling.
    
    Args:
        config_path (str): Path to configuration file
    
    Returns:
        dict: Parsed configuration
    """
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing configuration: {e}")
        raise
