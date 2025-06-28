#!/usr/bin/env python3
"""
Script to compare results from Pandas and Spark pipelines.
"""

import sys
import logging
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.utils import setup_logging, compare_datasets, save_metadata
from src.config import OUTPUT_DIR, LOGS_DIR

def main():
    """Main function to compare pipelines."""
    
    log_file = LOGS_DIR / "comparison.log"
    setup_logging(log_level="INFO", log_file=log_file)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting pipeline comparison")
        
        pandas_file = OUTPUT_DIR / "dataset_final.csv"
        spark_file = OUTPUT_DIR / "dataset_final_spark.csv"
        
        if not pandas_file.exists():
            logger.error(f"Pandas file not found: {pandas_file}")
            return
        
        if not spark_file.exists():
            logger.error(f"Spark file not found: {spark_file}")
            return
        
        logger.info("Loading datasets...")
        df_pandas = pd.read_csv(pandas_file)
        df_spark = pd.read_csv(spark_file)
        
        logger.info(f"Pandas dataset: {len(df_pandas):,} records")
        logger.info(f"Spark dataset: {len(df_spark):,} records")
        
        comparison = compare_datasets(
            df_pandas, df_spark, 
            name1="Pandas Pipeline", 
            name2="Spark Pipeline"
        )
        

        save_metadata(comparison, "pipeline_comparison.json")
        
        logger.info("Comparison summary:")
        
        if 'differences' in comparison and comparison['differences']:
            for diff_type, diff_info in comparison['differences'].items():
                logger.info(f"  {diff_type}: {diff_info}")
        else:
            logger.info("  No significant differences found")
        
        if 'clicked' in df_pandas.columns and 'clicked' in df_spark.columns:
            pandas_click_rate = df_pandas['clicked'].mean()
            spark_click_rate = df_spark['clicked'].mean()

            logger.info(f"  Pandas click rate: {pandas_click_rate:.4f}")
            logger.info(f"  Spark click rate: {spark_click_rate:.4f}")
            logger.info(f"  Difference: {abs(pandas_click_rate - spark_click_rate):.6f}")

        numeric_features = ['print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w']
        
        logger.info("  Numeric features comparison:")
        for feature in numeric_features:
            if feature in df_pandas.columns and feature in df_spark.columns:
                pandas_mean = df_pandas[feature].mean()
                spark_mean = df_spark[feature].mean()
                difference = abs(pandas_mean - spark_mean)
                
                logger.info(f"    {feature}:")
                logger.info(f"      Pandas mean: {pandas_mean:.4f}")
                logger.info(f"      Spark mean: {spark_mean:.4f}")
                logger.info(f"      Difference: {difference:.6f}")
        
        logger.info("  Comparison completed successfully")
        
    except Exception as e:
        logger.error(f"Error in comparison: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 