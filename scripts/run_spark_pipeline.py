#!/usr/bin/env python3
"""
Script to execute the Value Props Ranking Pipeline using PySpark.
"""

import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.spark_pipeline import SparkValuePropsPipeline
from src.utils import setup_logging, create_summary_report, save_metadata
from src.config import OUTPUT_DIR, LOGS_DIR

def main():
    """Main function to execute the Spark pipeline."""
    
    log_file = LOGS_DIR / "spark_pipeline.log"
    setup_logging(log_level="INFO", log_file=log_file)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Value Props Ranking Pipeline with Spark")
        
        spark_pipeline = SparkValuePropsPipeline()
        
        dataset = spark_pipeline.run_pipeline("dataset_final_spark.csv")
        
        pandas_dataset = dataset.select("*").toPandas()
        
        create_summary_report(pandas_dataset, "spark_summary_report.txt")
        
        metadata = {
            'pipeline_type': 'spark',
            'total_records': len(pandas_dataset),
            'total_columns': len(pandas_dataset.columns),
            'click_rate': float(pandas_dataset['clicked'].mean()),
            'unique_users': int(pandas_dataset['user_id'].nunique()),
            'unique_value_props': int(pandas_dataset['value_prop_id'].nunique())
        }
        save_metadata(metadata, "spark_dataset_metadata.json")
        
        logger.info("Spark pipeline completed successfully")
        logger.info(f"Dataset generated: {OUTPUT_DIR / 'dataset_final_spark.csv'}")
        logger.info(f"Records processed: {len(pandas_dataset):,}")
        logger.info(f"Click rate: {pandas_dataset['clicked'].mean():.2%}")
        
    except Exception as e:
        logger.error(f"Error in Spark pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 