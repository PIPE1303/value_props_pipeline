from pathlib import Path
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Directorios del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"
MODELS_DIR = PROJECT_ROOT / "models"

# Crear directorios si no existen
for directory in [OUTPUT_DIR, LOGS_DIR, MODELS_DIR]:
    directory.mkdir(exist_ok=True)

# Archivos de datos
PRINTS_FILE = DATA_DIR / "prints.json"
TAPS_FILE = DATA_DIR / "taps.json"
PAYS_FILE = DATA_DIR / "pays.csv"

# Configuración de procesamiento
WINDOW_DAYS = 21  # Ventana histórica en días
RECENT_DAYS = 7   # Días recientes para análisis

# Configuración de Spark (opcional)
SPARK_CONFIG = {
    "app_name": "ValuePropsPipeline",
    "master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

# Columnas finales del dataset
FINAL_COLUMNS = [
    "user_id", 
    "value_prop_id", 
    "timestamp", 
    "clicked",
    "print_count_3w", 
    "tap_count_3w", 
    "pay_count_3w", 
    "total_amount_3w"
]
