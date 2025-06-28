import pandas as pd
import logging
from pathlib import Path
from typing import Tuple
from .config import PRINTS_FILE, TAPS_FILE, PAYS_FILE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carga los datos de prints, taps y pays desde los archivos fuente.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: DataFrames de prints, taps y pays
    """
    try:
        logger.info("Cargando datos de prints...")
        prints = _load_prints()
        
        logger.info("Cargando datos de taps...")
        taps = _load_taps()
        
        logger.info("Cargando datos de pays...")
        pays = _load_pays()
        
        logger.info(f"Datos cargados exitosamente: {len(prints)} prints, {len(taps)} taps, {len(pays)} pays")
        return prints, taps, pays
        
    except Exception as e:
        logger.error(f"Error al cargar datos: {str(e)}")
        raise

def _load_prints() -> pd.DataFrame:
    """Carga y procesa el archivo de prints."""
    prints = pd.read_json(PRINTS_FILE, lines=True)
    prints['value_prop_id'] = prints['event_data'].apply(
        lambda x: x.get('value_prop') if isinstance(x, dict) else None
    )
    prints = prints.rename(columns={'day': 'timestamp'})
    prints['timestamp'] = pd.to_datetime(prints['timestamp'])
    return prints

def _load_taps() -> pd.DataFrame:
    """Carga y procesa el archivo de taps."""
    taps = pd.read_json(TAPS_FILE, lines=True)
    taps['value_prop_id'] = taps['event_data'].apply(
        lambda x: x.get('value_prop') if isinstance(x, dict) else None
    )
    taps = taps.rename(columns={'day': 'timestamp'})
    taps['timestamp'] = pd.to_datetime(taps['timestamp'])
    return taps

def _load_pays() -> pd.DataFrame:
    """Carga y procesa el archivo de pays."""
    pays = pd.read_csv(PAYS_FILE)
    pays = pays.rename(columns={
        'pay_date': 'timestamp',
        'value_prop': 'value_prop_id',
        'total': 'amount'
    })
    pays['timestamp'] = pd.to_datetime(pays['timestamp'])
    return pays

def save_dataset(df: pd.DataFrame, filename: str = "dataset_final.csv") -> None:
    """
    Guarda el dataset final en el directorio de salida.
    
    Args:
        df: DataFrame a guardar
        filename: Nombre del archivo de salida
    """
    from .config import OUTPUT_DIR
    
    output_path = OUTPUT_DIR / filename
    df.to_csv(output_path, index=False)
    logger.info(f"Dataset guardado en: {output_path}")

def validate_data(prints: pd.DataFrame, taps: pd.DataFrame, pays: pd.DataFrame) -> bool:
    """
    Valida que los datos cargados tengan la estructura esperada.
    
    Args:
        prints: DataFrame de prints
        taps: DataFrame de taps
        pays: DataFrame de pays
        
    Returns:
        bool: True si los datos son válidos
    """
    required_columns = {
        'prints': ['user_id', 'value_prop_id', 'timestamp'],
        'taps': ['user_id', 'value_prop_id', 'timestamp'],
        'pays': ['user_id', 'value_prop_id', 'timestamp', 'amount']
    }
    
    for df_name, df in [('prints', prints), ('taps', taps), ('pays', pays)]:
        missing_cols = set(required_columns[df_name]) - set(df.columns)
        if missing_cols:
            logger.error(f"Columnas faltantes en {df_name}: {missing_cols}")
            return False
    
    logger.info("Validación de datos exitosa")
    return True
