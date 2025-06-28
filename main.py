#!/usr/bin/env python3
"""
Script principal para ejecutar el pipeline de Value Props Ranking.
"""

import logging
import sys
from pathlib import Path

# Añadir el directorio src al path
sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline import run_pipeline, run_pipeline_with_validation
from src.config import OUTPUT_DIR

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Función principal del pipeline."""
    try:
        logger.info("Iniciando Value Props Ranking Pipeline")
        
        # Ejecutar pipeline con validaciones
        dataset = run_pipeline_with_validation()
        
        # Guardar resultado
        output_file = OUTPUT_DIR / "dataset_final.csv"
        dataset.to_csv(output_file, index=False)
        
        logger.info(f"Pipeline completado exitosamente")
        logger.info(f"Dataset generado: {output_file}")
        logger.info(f"Registros procesados: {len(dataset):,}")
        logger.info(f"Tasa de clicks: {dataset['clicked'].mean():.2%}")
        
        # Mostrar estadísticas básicas
        logger.info("Estadísticas del dataset:")
        logger.info(f"   - Usuarios únicos: {dataset['user_id'].nunique():,}")
        logger.info(f"   - Value props únicos: {dataset['value_prop_id'].nunique():,}")
        logger.info(f"   - Rango de fechas: {dataset['timestamp'].min()} a {dataset['timestamp'].max()}")
        
    except Exception as e:
        logger.error(f"Error en el pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
