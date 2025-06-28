"""
Utilidades generales para el proyecto Value Props Ranking.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

def setup_logging(log_level: str = "INFO", log_file: Optional[Path] = None) -> None:
    """
    Configura el sistema de logging.
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
        log_file: Archivo de log (opcional)
    """
    handlers = [logging.StreamHandler()]
    
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def calculate_data_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula métricas de calidad de datos.
    
    Args:
        df: DataFrame a analizar
        
    Returns:
        Dict con métricas de calidad
    """
    metrics = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': df.dtypes.to_dict()
    }
    
    missing_pct = (df.isnull().sum() / len(df) * 100).to_dict()
    metrics['missing_percentage'] = missing_pct
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        metrics['numeric_stats'] = df[numeric_cols].describe().to_dict()
    
    return metrics

def validate_dataset_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> bool:
    """
    Valida que el DataFrame tenga el esquema esperado.
    
    Args:
        df: DataFrame a validar
        expected_schema: Diccionario con columna: tipo_esperado
        
    Returns:
        bool: True si el esquema es válido
    """
    for column, expected_type in expected_schema.items():
        if column not in df.columns:
            logger.error(f"Columna faltante: {column}")
            return False
        
        actual_type = str(df[column].dtype)
        if expected_type not in actual_type:
            logger.warning(f"Tipo de columna {column}: esperado {expected_type}, actual {actual_type}")
    
    return True

def save_metadata(dataset_info: Dict[str, Any], filename: str = "dataset_metadata.json") -> None:
    """
    Guarda metadatos del dataset.
    
    Args:
        dataset_info: Información del dataset
        filename: Nombre del archivo de metadatos
    """
    from .config import OUTPUT_DIR
    
    metadata = {
        'created_at': datetime.now().isoformat(),
        'dataset_info': dataset_info
    }
    
    output_path = OUTPUT_DIR / filename
    with open(output_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    logger.info(f"Metadatos guardados en: {output_path}")

def compare_datasets(df1: pd.DataFrame, df2: pd.DataFrame, 
                    name1: str = "Dataset 1", name2: str = "Dataset 2") -> Dict[str, Any]:
    """
    Compara dos datasets y genera un reporte de diferencias.
    
    Args:
        df1: Primer DataFrame
        df2: Segundo DataFrame
        name1: Nombre del primer dataset
        name2: Nombre del segundo dataset
        
    Returns:
        Dict con reporte de comparación
    """
    comparison = {
        'dataset1': {
            'name': name1,
            'rows': len(df1),
            'columns': len(df1.columns),
            'columns_list': list(df1.columns)
        },
        'dataset2': {
            'name': name2,
            'rows': len(df2),
            'columns': len(df2.columns),
            'columns_list': list(df2.columns)
        },
        'differences': {}
    }
    
    if len(df1) != len(df2):
        comparison['differences']['row_count'] = {
            'df1': len(df1),
            'df2': len(df2),
            'difference': abs(len(df1) - len(df2))
        }
    
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    if cols1 != cols2:
        comparison['differences']['columns'] = {
            'only_in_df1': list(cols1 - cols2),
            'only_in_df2': list(cols2 - cols1),
            'common': list(cols1 & cols2)
        }
    
    common_cols = cols1 & cols2
    if common_cols:
        comparison['differences']['value_comparison'] = {}
        
        for col in common_cols:
            if df1[col].dtype == df2[col].dtype:
                if df1[col].dtype in ['object', 'string']:
                    comparison['differences']['value_comparison'][col] = {
                        'unique_values_df1': df1[col].nunique(),
                        'unique_values_df2': df2[col].nunique()
                    }
                else:
                    comparison['differences']['value_comparison'][col] = {
                        'mean_df1': df1[col].mean(),
                        'mean_df2': df2[col].mean(),
                        'std_df1': df1[col].std(),
                        'std_df2': df2[col].std()
                    }
    
    return comparison

def create_summary_report(df: pd.DataFrame, output_file: str = "summary_report.txt") -> None:
    """
    Crea un reporte resumen del dataset.
    
    Args:
        df: DataFrame a analizar
        output_file: Nombre del archivo de reporte
    """
    from .config import OUTPUT_DIR
    
    report_lines = [
        "=" * 50,
        "REPORTE RESUMEN DEL DATASET",
        "=" * 50,
        f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total de registros: {len(df):,}",
        f"Total de columnas: {len(df.columns)}",
        "",
        "COLUMNAS:",
        "-" * 20
    ]
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        
        if df[col].dtype in ['object', 'string']:
            unique_count = df[col].nunique()
            report_lines.append(f"{col}: {dtype} | Nulls: {null_count} ({null_pct:.1f}%) | Únicos: {unique_count}")
        else:
            mean_val = df[col].mean()
            std_val = df[col].std()
            report_lines.append(f"{col}: {dtype} | Nulls: {null_count} ({null_pct:.1f}%) | Mean: {mean_val:.2f} | Std: {std_val:.2f}")
    
    if 'clicked' in df.columns:
        click_rate = df['clicked'].mean() * 100
        report_lines.extend([
            "",
            "ESTADÍSTICAS DE CLICK:",
            "-" * 20,
            f"Tasa de click: {click_rate:.2f}%",
            f"Total clicks: {df['clicked'].sum():,}",
            f"Total impresiones: {len(df):,}"
        ])
    
    if 'user_id' in df.columns:
        unique_users = df['user_id'].nunique()
        report_lines.extend([
            "",
            "ESTADÍSTICAS DE USUARIOS:",
            "-" * 20,
            f"Usuarios únicos: {unique_users:,}",
            f"Promedio de registros por usuario: {len(df) / unique_users:.2f}"
        ])
    
    output_path = OUTPUT_DIR / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"Reporte resumen guardado en: {output_path}")

def get_memory_usage(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula el uso de memoria del DataFrame.
    
    Args:
        df: DataFrame a analizar
        
    Returns:
        Dict con información de uso de memoria
    """
    memory_usage = df.memory_usage(deep=True)
    
    return {
        'total_memory_mb': memory_usage.sum() / 1024 / 1024,
        'memory_per_column': memory_usage.to_dict(),
        'memory_efficiency': {
            'total_memory_mb': memory_usage.sum() / 1024 / 1024,
            'memory_per_row_kb': (memory_usage.sum() / len(df)) / 1024
        }
    } 