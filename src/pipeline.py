import pandas as pd
import logging
from typing import Optional
from .io_utils import load_data, save_dataset, validate_data
from .feature_engineering import create_features_pipeline, get_feature_statistics
from .config import FINAL_COLUMNS

logger = logging.getLogger(__name__)

def build_dataset(end_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Build the final dataset with all features.
    
    Args:
        end_date: End date for analysis (optional)
        
    Returns:
        pd.DataFrame: Final dataset with features
    """
    logger.info("Starting dataset construction...")
    
    try:
        prints, taps, pays = load_data()
        
        if not validate_data(prints, taps, pays):
            raise ValueError("Data validation failed")
        
        dataset = create_features_pipeline(prints, taps, pays, end_date)
        
        final_dataset = dataset[FINAL_COLUMNS].copy()
        
        stats = get_feature_statistics(final_dataset)
        logger.info("Dataset statistics:")
        for feature, stat in stats.items():
            logger.info(f"  {feature}: mean={stat['mean']:.2f}, std={stat['std']:.2f}")
        
        logger.info(f"Dataset built successfully with {len(final_dataset)} records")
        return final_dataset
        
    except Exception as e:
        logger.error(f"Error in dataset construction: {str(e)}")
        raise

def run_pipeline(output_filename: str = "dataset_final.csv") -> None:
    """
    Execute the complete pipeline and save the result.
    
    Args:
        output_filename: Output filename
    """
    logger.info("Executing complete pipeline...")
    
    try:
        dataset = build_dataset()
        
        save_dataset(dataset, output_filename)
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

def run_pipeline_with_validation() -> pd.DataFrame:
    """
    Execute the pipeline with additional validations.
    
    Returns:
        pd.DataFrame: Validated final dataset
    """
    logger.info("Executing pipeline with validations...")
    
    dataset = build_dataset()
    
    logger.info("Performing additional validations...")
    
    critical_cols = ['user_id', 'value_prop_id', 'timestamp']
    for col in critical_cols:
        if dataset[col].isnull().any():
            logger.warning(f"NaN values found in critical column: {col}")
    
    if not set(dataset['clicked'].unique()).issubset({0, 1}):
        logger.warning("'clicked' column is not binary")
    
    numeric_features = ['print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w']
    for feature in numeric_features:
        if (dataset[feature] < 0).any():
            logger.warning(f"Negative values found in {feature}")
    
    logger.info("Validations completed")
    return dataset
