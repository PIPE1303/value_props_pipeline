import pandas as pd
import logging
from typing import Optional
from .io_utils import load_data, save_dataset, validate_data
from .feature_engineering import create_features_pipeline, get_feature_statistics
from .config import FINAL_COLUMNS

logger = logging.getLogger(__name__)

def build_dataset(end_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Construye el dataset final con todas las features.
    
    Args:
        end_date: Fecha final para el análisis (opcional)
        
    Returns:
        pd.DataFrame: Dataset final con features
    """
    logger.info("Iniciando construcción del dataset...")
    
    try:
        # 1. Cargar datos
        prints, taps, pays = load_data()
        
        # 2. Validar datos
        if not validate_data(prints, taps, pays):
            raise ValueError("Los datos no pasaron la validación")
        
        # 3. Crear features
        dataset = create_features_pipeline(prints, taps, pays, end_date)
        
        # 4. Seleccionar columnas finales
        final_dataset = dataset[FINAL_COLUMNS].copy()
        
        # 5. Mostrar estadísticas
        stats = get_feature_statistics(final_dataset)
        logger.info("Estadísticas del dataset:")
        for feature, stat in stats.items():
            logger.info(f"  {feature}: mean={stat['mean']:.2f}, std={stat['std']:.2f}")
        
        logger.info(f"Dataset construido exitosamente con {len(final_dataset)} registros")
        return final_dataset
        
    except Exception as e:
        logger.error(f"Error en la construcción del dataset: {str(e)}")
        raise

def run_pipeline(output_filename: str = "dataset_final.csv") -> None:
    """
    Ejecuta el pipeline completo y guarda el resultado.
    
    Args:
        output_filename: Nombre del archivo de salida
    """
    logger.info("Ejecutando pipeline completo...")
    
    try:
        # Construir dataset
        dataset = build_dataset()
        
        # Guardar resultado
        save_dataset(dataset, output_filename)
        
        logger.info("Pipeline completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error en el pipeline: {str(e)}")
        raise

def run_pipeline_with_validation() -> pd.DataFrame:
    """
    Ejecuta el pipeline con validaciones adicionales.
    
    Returns:
        pd.DataFrame: Dataset final validado
    """
    logger.info("Ejecutando pipeline con validaciones...")
    
    # Construir dataset
    dataset = build_dataset()
    
    # Validaciones adicionales
    logger.info("Realizando validaciones adicionales...")
    
    # Verificar que no hay valores NaN en columnas críticas
    critical_cols = ['user_id', 'value_prop_id', 'timestamp']
    for col in critical_cols:
        if dataset[col].isnull().any():
            logger.warning(f"Valores NaN encontrados en columna crítica: {col}")
    
    # Verificar que clicked es binario
    if not set(dataset['clicked'].unique()).issubset({0, 1}):
        logger.warning("La columna 'clicked' no es binaria")
    
    # Verificar que las features numéricas son no-negativas
    numeric_features = ['print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w']
    for feature in numeric_features:
        if (dataset[feature] < 0).any():
            logger.warning(f"Valores negativos encontrados en {feature}")
    
    logger.info("Validaciones completadas")
    return dataset
