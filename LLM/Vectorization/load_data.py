import glob
import json
import os
from config import get_preprocessed_input_path

class AlertLoader:
    def __init__(self):
        # Load the input path from config.py using get_preprocessed_input_path
        self.input_path = str(get_preprocessed_input_path())
        
    def load_alerts(self, logger=None):
        alerts = []
        pattern = os.path.join(self.input_path,'*.json')
        for filepath in glob.iglob(pattern, recursive=True):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        alerts.extend(data)
                    elif isinstance(data, dict):
                        alerts.append(data)
                    else:
                        if logger:
                            logger.warning(f"Unexpected JSON structure in file {filepath}")
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to load {filepath}: {e}")
        return alerts

