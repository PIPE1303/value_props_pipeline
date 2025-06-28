from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Project directories
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"
MODELS_DIR = PROJECT_ROOT / "models"

# Create directories if they don't exist
for directory in [OUTPUT_DIR, LOGS_DIR, MODELS_DIR]:
    directory.mkdir(exist_ok=True)

# Data files
PRINTS_FILE = DATA_DIR / "prints.json"
TAPS_FILE = DATA_DIR / "taps.json"
PAYS_FILE = DATA_DIR / "pays.csv"

# Processing configuration
WINDOW_DAYS = 21  # Historical window in days
RECENT_DAYS = 7   # Recent days for analysis

# Spark configuration (optional)
SPARK_CONFIG = {
    "app_name": "ValuePropsPipeline",
    "master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

# Final dataset columns
FINAL_COLUMNS = [
    "user_id", 
    "value_prop_id", 
    "timestamp", 
    "clicked",
    "print_count_3w", 
    "tap_count_3w", 
    "pay_count_3w", 
    "total_amount_3w"
]
