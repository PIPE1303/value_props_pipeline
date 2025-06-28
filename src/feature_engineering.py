import pandas as pd
import numpy as np
from datetime import timedelta
import logging
from typing import Dict, Any
from .config import WINDOW_DAYS, RECENT_DAYS

logger = logging.getLogger(__name__)

def add_click_flag(prints_df: pd.DataFrame, taps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'clicked' column based on matches with taps.
    
    Args:
        prints_df: Prints DataFrame
        taps_df: Taps DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with 'clicked' column added
    """
    logger.info("Adding click flag...")
    
    taps_set = taps_df[['user_id', 'value_prop_id', 'timestamp']].drop_duplicates()
    
    result = prints_df.merge(
        taps_set,
        on=['user_id', 'value_prop_id', 'timestamp'],
        how='left',
        indicator='click_flag'
    )
    
    result['clicked'] = (result['click_flag'] == 'both').astype(int)
    result = result.drop(columns=['click_flag'])
    
    logger.info(f"Click flag added. Clicks found: {result['clicked'].sum()}")
    return result

def add_historical_features(
    df: pd.DataFrame, 
    source_df: pd.DataFrame, 
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    features_config: Dict[str, Dict[str, str]]
) -> pd.DataFrame:
    """
    Add historical features based on time windows.
    
    Args:
        df: Main DataFrame
        source_df: Source DataFrame for feature calculation
        window_start: Start of time window
        window_end: End of time window
        features_config: Configuration of features to calculate
        
    Returns:
        pd.DataFrame: DataFrame with historical features added
    """
    logger.info(f"Adding historical features from {window_start} to {window_end}")
    
    mask = (source_df['timestamp'] >= window_start) & (source_df['timestamp'] < window_end)
    filtered_source = source_df[mask].copy()
    
    if filtered_source.empty:
        logger.warning("No data in specified time window")
        for feature_name in features_config.keys():
            df[feature_name] = 0
        return df
    
    grouped = filtered_source.groupby(['user_id', 'value_prop_id'])
    
    features_df = pd.DataFrame()
    
    for feature_name, config in features_config.items():
        if config['agg_func'] == 'count':
            features_df[feature_name] = grouped.size()
        elif config['agg_func'] == 'sum':
            features_df[feature_name] = grouped[config['column']].sum()
        elif config['agg_func'] == 'mean':
            features_df[feature_name] = grouped[config['column']].mean()
        else:
            raise ValueError(f"Unsupported aggregation function: {config['agg_func']}")
    
    features_df = features_df.reset_index()
    
    result = df.merge(features_df, on=['user_id', 'value_prop_id'], how='left')
    
    for feature_name in features_config.keys():
        result[feature_name] = result[feature_name].fillna(0)
    
    logger.info(f"Historical features added: {list(features_config.keys())}")
    return result

def create_features_pipeline(
    prints_df: pd.DataFrame,
    taps_df: pd.DataFrame,
    pays_df: pd.DataFrame,
    end_date: pd.Timestamp = None
) -> pd.DataFrame:
    """
    Complete feature creation pipeline.
    
    Args:
        prints_df: Prints DataFrame
        taps_df: Taps DataFrame
        pays_df: Pays DataFrame
        end_date: End date for analysis (default: max of prints)
        
    Returns:
        pd.DataFrame: DataFrame with all features
    """
    logger.info("Starting features pipeline...")
    
    if end_date is None:
        end_date = prints_df['timestamp'].max()
    
    start_last_week = end_date - timedelta(days=RECENT_DAYS)
    start_3weeks_ago = end_date - timedelta(days=WINDOW_DAYS)
    
    logger.info(f"Analysis window: {start_last_week} to {end_date}")
    logger.info(f"Historical window: {start_3weeks_ago} to {start_last_week}")
    
    recent_prints = prints_df[
        (prints_df['timestamp'] >= start_last_week) & 
        (prints_df['timestamp'] <= end_date)
    ].copy()
    
    logger.info(f"Recent prints found: {len(recent_prints)}")
    
    recent_prints = add_click_flag(recent_prints, taps_df)
    
    features_config = {
        'print_count_3w': {'agg_func': 'count'},
        'tap_count_3w': {'agg_func': 'count'},
        'pay_count_3w': {'agg_func': 'count'},
        'total_amount_3w': {'agg_func': 'sum', 'column': 'amount'}
    }

    recent_prints = add_historical_features(
        recent_prints, prints_df, start_3weeks_ago, start_last_week,
        {'print_count_3w': {'agg_func': 'count'}}
    )
    
    recent_prints = add_historical_features(
        recent_prints, taps_df, start_3weeks_ago, start_last_week,
        {'tap_count_3w': {'agg_func': 'count'}}
    )
    
    recent_prints = add_historical_features(
        recent_prints, pays_df, start_3weeks_ago, start_last_week,
        {
            'pay_count_3w': {'agg_func': 'count'},
            'total_amount_3w': {'agg_func': 'sum', 'column': 'amount'}
        }
    )
    
    logger.info("Features pipeline completed successfully")
    return recent_prints

def get_feature_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate descriptive statistics of features.
    
    Args:
        df: DataFrame with features
        
    Returns:
        Dict: Feature statistics
    """
    feature_cols = ['print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w']
    
    stats = {}
    for col in feature_cols:
        if col in df.columns:
            stats[col] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'max': df[col].max(),
                'median': df[col].median()
            }
    
    return stats
