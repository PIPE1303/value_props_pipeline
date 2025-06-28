#!/usr/bin/env python3
"""
Script de limpieza para el proyecto Value Props Ranking.
"""

import shutil
import os
from pathlib import Path
import logging

def cleanup_project():
    """Limpia archivos temporales y generados del proyecto."""
    
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Directorios a limpiar
    dirs_to_clean = [
        "output",
        "logs", 
        "models",
        "__pycache__",
        "src/__pycache__",
        "tests/__pycache__",
        "scripts/__pycache__",
        ".pytest_cache",
        "htmlcov",
        ".coverage",
        "spark-warehouse",
        "derby.log"
    ]
    
    # Archivos a eliminar
    files_to_remove = [
        "*.pyc",
        "*.pyo", 
        "*.pyd",
        "*.so",
        "*.egg",
        "*.egg-info",
        "*.log",
        "*.tmp",
        "*.temp",
        "*.bak",
        "*.backup"
    ]
    
    logger.info("🧹 Iniciando limpieza del proyecto...")
    
    # Limpiar directorios
    for dir_path in dirs_to_clean:
        if os.path.exists(dir_path):
            try:
                if os.path.isdir(dir_path):
                    shutil.rmtree(dir_path)
                    logger.info(f"✅ Directorio eliminado: {dir_path}")
                else:
                    os.remove(dir_path)
                    logger.info(f"✅ Archivo eliminado: {dir_path}")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar {dir_path}: {e}")
    
    # Crear directorios necesarios
    required_dirs = ["output", "logs", "models"]
    for dir_path in required_dirs:
        Path(dir_path).mkdir(exist_ok=True)
        logger.info(f"📁 Directorio creado/verificado: {dir_path}")
    
    # Buscar y eliminar archivos temporales
    for pattern in files_to_remove:
        for root, dirs, files in os.walk("."):
            for file in files:
                if file.endswith(pattern.replace("*", "")):
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logger.info(f"🗑️ Archivo eliminado: {file_path}")
                    except Exception as e:
                        logger.warning(f"⚠️ No se pudo eliminar {file_path}: {e}")
    
    logger.info("✨ Limpieza completada exitosamente!")

if __name__ == "__main__":
    cleanup_project() 