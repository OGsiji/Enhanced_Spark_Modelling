import os
import pytest
import pandas as pd
import datetime
from src.data_processing.data_generator import TaskLogGenerator
from src.utils.config import load_config

def test_task_log_generator():
    # Load test configuration
    config = load_config()
    
    # Modify config for test (smaller dataset)
    config['data']['generator']['num_records'] = 10_000
    config['data']['generator']['output_path'] = "./test_task_logs.parquet"
    
    # Create generator
    generator = TaskLogGenerator(config)
    
    # Generate task logs
    df = generator.generate_task_logs()
    
    # Save to Parquet
    generator.save_to_parquet(df)
    
    # Validate generated data
    assert len(df) == 10_000, "Incorrect number of records generated"
    
    # Check required columns
    required_columns = ['task_id', 'project', 'user', 'start_time', 'end_time', 'duration']
    for col in required_columns:
        assert col in df.columns, f"Missing column: {col}"
    
    # Validate data types and constraints
    assert df['duration'].min() >= 0, "Negative durations not allowed"
    assert df['start_time'].min() > datetime(2022, 1, 1), "Unrealistic start times"
    
    # Clean up test file
    os.remove("./test_task_logs.parquet")

def test_data_generation_parameters():
    config = load_config()
    
    # Check configuration parameters
    assert 'projects' in config, "Projects not defined in config"
    assert 'users' in config, "Users not defined in config"
    assert len(config['projects']) > 0, "No projects defined"
    assert config['users'][1]['count'] > 0, "User count must be positive"