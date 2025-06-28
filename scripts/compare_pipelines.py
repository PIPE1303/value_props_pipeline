#!/usr/bin/env python3
"""
Script para comparar los resultados de los pipelines de Pandas y Spark.
"""

import sys
import logging
import pandas as pd
from pathlib import Path

# Añadir el directorio src al path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.utils import setup_logging, compare_datasets, save_metadata
from src.config import OUTPUT_DIR

def main():
    """Función principal para comparar pipelines."""
    
    # Configurar logging
    log_file = OUTPUT_DIR / "comparison.log"
    setup_logging(log_level="INFO", log_file=log_file)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🔍 Iniciando comparación de pipelines")
        
        # Cargar datasets
        pandas_file = OUTPUT_DIR / "dataset_final.csv"
        spark_file = OUTPUT_DIR / "dataset_final_spark.csv"
        
        if not pandas_file.exists():
            logger.error(f"Archivo de Pandas no encontrado: {pandas_file}")
            return
        
        if not spark_file.exists():
            logger.error(f"Archivo de Spark no encontrado: {spark_file}")
            return
        
        logger.info("Cargando datasets...")
        df_pandas = pd.read_csv(pandas_file)
        df_spark = pd.read_csv(spark_file)
        
        logger.info(f"Dataset Pandas: {len(df_pandas):,} registros")
        logger.info(f"Dataset Spark: {len(df_spark):,} registros")
        
        # Comparar datasets
        comparison = compare_datasets(
            df_pandas, df_spark, 
            name1="Pandas Pipeline", 
            name2="Spark Pipeline"
        )
        
        # Guardar comparación
        save_metadata(comparison, "pipeline_comparison.json")
        
        # Mostrar resumen de diferencias
        logger.info("📊 Resumen de comparación:")
        
        if 'differences' in comparison and comparison['differences']:
            for diff_type, diff_info in comparison['differences'].items():
                logger.info(f"  {diff_type}: {diff_info}")
        else:
            logger.info("  ✅ No se encontraron diferencias significativas")
        
        # Comparar estadísticas específicas
        if 'clicked' in df_pandas.columns and 'clicked' in df_spark.columns:
            pandas_click_rate = df_pandas['clicked'].mean()
            spark_click_rate = df_spark['clicked'].mean()
            
            logger.info(f"  Tasa de click Pandas: {pandas_click_rate:.4f}")
            logger.info(f"  Tasa de click Spark: {spark_click_rate:.4f}")
            logger.info(f"  Diferencia: {abs(pandas_click_rate - spark_click_rate):.6f}")
        
        # Comparar features numéricas
        numeric_features = ['print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w']
        
        logger.info("  Comparación de features numéricas:")
        for feature in numeric_features:
            if feature in df_pandas.columns and feature in df_spark.columns:
                pandas_mean = df_pandas[feature].mean()
                spark_mean = df_spark[feature].mean()
                difference = abs(pandas_mean - spark_mean)
                
                logger.info(f"    {feature}:")
                logger.info(f"      Pandas mean: {pandas_mean:.4f}")
                logger.info(f"      Spark mean: {spark_mean:.4f}")
                logger.info(f"      Diferencia: {difference:.6f}")
        
        logger.info("✅ Comparación completada exitosamente")
        
    except Exception as e:
        logger.error(f"❌ Error en la comparación: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 