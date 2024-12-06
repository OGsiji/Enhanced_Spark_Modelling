import os
import random
from datetime import datetime, timedelta
import sys

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)


from faker import Faker
import pandas as pd
import numpy as np

class TaskLogGenerator:
    def __init__(self, config):
        """
        Initialize the task log generator with configuration
        
        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.faker = Faker()
        
        # Extract configuration parameters
        self.num_records = config['data']['generator']['num_records']
        self.output_path = config['data']['generator']['output_path']
        self.projects = config['projects']
        self.user_prefix = config['users'][0]['prefix']
        self.user_count = config['users'][1]['count']
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    def generate_task_logs(self):
        """
        Generate synthetic task log data
        
        Returns:
            pd.DataFrame: Generated task log dataframe
        """
        # Preallocate arrays for efficiency
        task_ids = [f"task_{i}" for i in range(self.num_records)]
        projects = np.random.choice(self.projects, self.num_records)
        users = [f"{self.user_prefix}_{random.randint(1, self.user_count)}" for _ in range(self.num_records)]
        
        # Generate timestamps with more realistic distribution
        base_time = datetime.now() - timedelta(days=365)
        start_times = [base_time + timedelta(minutes=random.randint(0, 525600)) for _ in range(self.num_records)]
        
        # Duration with a more natural distribution (log-normal for task durations)
        durations = np.random.lognormal(mean=np.log(2), sigma=1, size=self.num_records)
        
        # Create DataFrame
        df = pd.DataFrame({
            'task_id': task_ids,
            'project': projects,
            'user': users,
            'start_time': start_times,
            'duration': durations,
        })
        
        # Calculate end times
        df['end_time'] = df['start_time'] + pd.to_timedelta(df['duration'], unit='h')
        
        return df



    def save_to_parquet(self, df):
        """
        Save generated dataframe to Parquet
        
        Args:
            df (pd.DataFrame): Task log dataframe
        """
        # Ensure timestamp columns are in microsecond precision
        timestamp_columns = ['start_time', 'end_time']
        for col in timestamp_columns:
            # Round to microseconds to avoid precision loss
            df[col] = df[col].dt.floor('us')
        
        # Optional: You can also specify date_unit if needed
        df.to_parquet(self.output_path, index=False)
        print(f"Generated {len(df)} task log records at {self.output_path}")

def main():
    # Load configuration 
    from utils.config import load_config
    
    config = load_config()
    generator = TaskLogGenerator(config)
    
    # Generate and save task logs
    task_logs = generator.generate_task_logs()
    generator.save_to_parquet(task_logs)

if __name__ == "__main__":
    main()
