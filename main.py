#!/usr/bin/env python3
"""
Main script to execute the Value Props Ranking Pipeline.
"""

import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline import run_pipeline, run_pipeline_with_validation
from src.config import OUTPUT_DIR, LOGS_DIR

# Configure logging before executing the pipeline
log_file = LOGS_DIR / "pipeline.log"
log_file.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main pipeline function."""
    try:
        logger.info("Starting Value Props Ranking Pipeline")
        
        dataset = run_pipeline_with_validation()
        
        output_file = OUTPUT_DIR / "dataset_final.csv"
        dataset.to_csv(output_file, index=False)
        
        logger.info(f"Pipeline completed successfully")
        logger.info(f"Dataset generated: {output_file}")
        logger.info(f"Records processed: {len(dataset):,}")
        logger.info(f"Click rate: {dataset['clicked'].mean():.2%}")
        
        logger.info("Dataset statistics:")
        logger.info(f"   - Unique users: {dataset['user_id'].nunique():,}")
        logger.info(f"   - Unique value props: {dataset['value_prop_id'].nunique():,}")
        logger.info(f"   - Date range: {dataset['timestamp'].min()} to {dataset['timestamp'].max()}")
        
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
