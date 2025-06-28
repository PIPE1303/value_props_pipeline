import pandas as pd
import numpy as np
from datetime import timedelta
import logging
from typing import Dict, Any
from .config import WINDOW_DAYS, RECENT_DAYS

logger = logging.getLogger(__name__)

def add_click_flag(prints_df: pd.DataFrame, taps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade la columna 'clicked' basada en la coincidencia con taps.
    
    Args:
        prints_df: DataFrame de prints
        taps_df: DataFrame de taps
        
    Returns:
        pd.DataFrame: DataFrame con columna 'clicked' añadida
    """
    logger.info("Añadiendo flag de click...")
    
    taps_set = taps_df[['user_id', 'value_prop_id', 'timestamp']].drop_duplicates()
    
    result = prints_df.merge(
        taps_set,
        on=['user_id', 'value_prop_id', 'timestamp'],
        how='left',
        indicator='click_flag'
    )
    
    result['clicked'] = (result['click_flag'] == 'both').astype(int)
    result = result.drop(columns=['click_flag'])
    
    logger.info(f"Flag de click añadido. Clicks encontrados: {result['clicked'].sum()}")
    return result

def add_historical_features(
    df: pd.DataFrame, 
    source_df: pd.DataFrame, 
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    features_config: Dict[str, Dict[str, str]]
) -> pd.DataFrame:
    """
    Añade features históricas basadas en ventanas temporales.
    
    Args:
        df: DataFrame principal
        source_df: DataFrame fuente para calcular features
        window_start: Inicio de la ventana temporal
        window_end: Fin de la ventana temporal
        features_config: Configuración de features a calcular
        
    Returns:
        pd.DataFrame: DataFrame con features históricas añadidas
    """
    logger.info(f"Añadiendo features históricas desde {window_start} hasta {window_end}")
    
    mask = (source_df['timestamp'] >= window_start) & (source_df['timestamp'] < window_end)
    filtered_source = source_df[mask].copy()
    
    if filtered_source.empty:
        logger.warning("No hay datos en la ventana temporal especificada")
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
            raise ValueError(f"Función de agregación no soportada: {config['agg_func']}")
    
    features_df = features_df.reset_index()
    
    result = df.merge(features_df, on=['user_id', 'value_prop_id'], how='left')
    
    for feature_name in features_config.keys():
        result[feature_name] = result[feature_name].fillna(0)
    
    logger.info(f"Features históricas añadidas: {list(features_config.keys())}")
    return result

def create_features_pipeline(
    prints_df: pd.DataFrame,
    taps_df: pd.DataFrame,
    pays_df: pd.DataFrame,
    end_date: pd.Timestamp = None
) -> pd.DataFrame:
    """
    Pipeline completo de creación de features.
    
    Args:
        prints_df: DataFrame de prints
        taps_df: DataFrame de taps
        pays_df: DataFrame de pays
        end_date: Fecha final para el análisis (por defecto max de prints)
        
    Returns:
        pd.DataFrame: DataFrame con todas las features
    """
    logger.info("Iniciando pipeline de features...")
    
    if end_date is None:
        end_date = prints_df['timestamp'].max()
    
    start_last_week = end_date - timedelta(days=RECENT_DAYS)
    start_3weeks_ago = end_date - timedelta(days=WINDOW_DAYS)
    
    logger.info(f"Ventana de análisis: {start_last_week} a {end_date}")
    logger.info(f"Ventana histórica: {start_3weeks_ago} a {start_last_week}")
    
    recent_prints = prints_df[
        (prints_df['timestamp'] >= start_last_week) & 
        (prints_df['timestamp'] <= end_date)
    ].copy()
    
    logger.info(f"Prints recientes encontrados: {len(recent_prints)}")
    
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
    
    logger.info("Pipeline de features completado exitosamente")
    return recent_prints

def get_feature_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula estadísticas descriptivas de las features.
    
    Args:
        df: DataFrame con features
        
    Returns:
        Dict: Estadísticas de las features
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
