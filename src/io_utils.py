import pandas as pd
import logging
from pathlib import Path
from typing import Tuple
from .config import PRINTS_FILE, TAPS_FILE, PAYS_FILE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load prints, taps, and pays data from source files.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: DataFrames for prints, taps, and pays
    """
    try:
        logger.info("Loading prints data...")
        prints = _load_prints()
        
        logger.info("Loading taps data...")
        taps = _load_taps()
        
        logger.info("Loading pays data...")
        pays = _load_pays()
        
        logger.info(f"Data loaded successfully: {len(prints)} prints, {len(taps)} taps, {len(pays)} pays")
        return prints, taps, pays
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def _load_prints() -> pd.DataFrame:
    """Load and process prints file."""
    prints = pd.read_json(PRINTS_FILE, lines=True)
    prints['value_prop_id'] = prints['event_data'].apply(
        lambda x: x.get('value_prop') if isinstance(x, dict) else None
    )
    prints = prints.rename(columns={'day': 'timestamp'})
    prints['timestamp'] = pd.to_datetime(prints['timestamp'])
    return prints

def _load_taps() -> pd.DataFrame:
    """Load and process taps file."""
    taps = pd.read_json(TAPS_FILE, lines=True)
    taps['value_prop_id'] = taps['event_data'].apply(
        lambda x: x.get('value_prop') if isinstance(x, dict) else None
    )
    taps = taps.rename(columns={'day': 'timestamp'})
    taps['timestamp'] = pd.to_datetime(taps['timestamp'])
    return taps

def _load_pays() -> pd.DataFrame:
    """Load and process pays file."""
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
    Save the final dataset to the output directory.
    
    Args:
        df: DataFrame to save
        filename: Output filename
    """
    from .config import OUTPUT_DIR
    
    output_path = OUTPUT_DIR / filename
    df.to_csv(output_path, index=False)
    logger.info(f"Dataset saved to: {output_path}")

def validate_data(prints: pd.DataFrame, taps: pd.DataFrame, pays: pd.DataFrame) -> bool:
    """
    Validate that loaded data has the expected structure.
    
    Args:
        prints: Prints DataFrame
        taps: Taps DataFrame
        pays: Pays DataFrame
        
    Returns:
        bool: True if data is valid
    """
    required_columns = {
        'prints': ['user_id', 'value_prop_id', 'timestamp'],
        'taps': ['user_id', 'value_prop_id', 'timestamp'],
        'pays': ['user_id', 'value_prop_id', 'timestamp', 'amount']
    }
    
    for df_name, df in [('prints', prints), ('taps', taps), ('pays', pays)]:
        missing_cols = set(required_columns[df_name]) - set(df.columns)
        if missing_cols:
            logger.error(f"Missing columns in {df_name}: {missing_cols}")
            return False
    
    logger.info("Data validation successful")
    return True
