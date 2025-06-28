#!/usr/bin/env python3
"""
Script para ejecutar el pipeline de Value Props Ranking usando PySpark.
"""

import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.spark_pipeline import SparkValuePropsPipeline
from src.utils import setup_logging, create_summary_report, save_metadata
from src.config import OUTPUT_DIR, LOGS_DIR

def main():
    """Función principal para ejecutar el pipeline de Spark."""
    
    log_file = LOGS_DIR / "spark_pipeline.log"
    setup_logging(log_level="INFO", log_file=log_file)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Iniciando Value Props Ranking Pipeline con Spark")
        
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
        
        logger.info("Pipeline de Spark completado exitosamente")
        logger.info(f"Dataset generado: {OUTPUT_DIR / 'dataset_final_spark.csv'}")
        logger.info(f"Registros procesados: {len(pandas_dataset):,}")
        logger.info(f"Tasa de clicks: {pandas_dataset['clicked'].mean():.2%}")
        
    except Exception as e:
        logger.error(f"Error en el pipeline de Spark: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 