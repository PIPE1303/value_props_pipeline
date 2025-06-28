#!/usr/bin/env python3
"""
Cleanup script for the Value Props Ranking project.
"""

import shutil
import os
from pathlib import Path
import logging

def cleanup_project():
    """Clean temporary and generated files from the project."""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
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
    
    logger.info("Starting project cleanup...")
    
    for dir_path in dirs_to_clean:
        if os.path.exists(dir_path):
            try:
                if os.path.isdir(dir_path):
                    shutil.rmtree(dir_path)
                    logger.info(f"Directory removed: {dir_path}")
                else:
                    os.remove(dir_path)
                    logger.info(f"File removed: {dir_path}")
            except Exception as e:
                logger.warning(f"Could not remove {dir_path}: {e}")
    
    required_dirs = ["output", "logs", "models"]
    for dir_path in required_dirs:
        Path(dir_path).mkdir(exist_ok=True)
        logger.info(f"Directory created/verified: {dir_path}")
    
    for pattern in files_to_remove:
        for root, dirs, files in os.walk("."):
            for file in files:
                if file.endswith(pattern.replace("*", "")):
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logger.info(f"File removed: {file_path}")
                    except Exception as e:
                        logger.warning(f"Could not remove {file_path}: {e}")
    
    logger.info("Cleanup completed successfully!")

if __name__ == "__main__":
    cleanup_project() 