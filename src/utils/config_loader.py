import yaml
import os
import logging
from datetime import datetime
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_table_config():
    """
    Load table configuration from YAML file
    Returns tuple of (table_configs, extract_templates)
    """
    # ✅ FIX: Path yang reliable di Docker environment
    config_path = '/opt/airflow/config/tables.yaml'
    
    # Fallback ke relative path jika absolute tidak ada
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(__file__), '../../config/tables.yaml')
        logger.warning(f"Using fallback path: {config_path}")
    
    try:
        logger.info(f"Loading config from: {config_path}")
        
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            return {}, {}
            
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        
        # Ambil table configs tables.yaml dari key 'tables'
        table_configs = config.get('tables', {})
        extract_templates = config.get('extract_templates', {})
        
        logger.info(f"✅ Loaded {len(table_configs)} table configs")
        logger.info(f"Table configs: {list(table_configs.keys())}")
        
        return table_configs, extract_templates
        
    except Exception as e:
        logger.error(f"❌ Error loading table config: {e}", exc_info=True)
        return {}, {}

def get_next_month_to_process(table_name, target_year):
    """Get next month to process for incremental monthly ETL dengan validasi"""
    try:
        variable_key = f"etl_last_month_{table_name}_{target_year}"
        last_processed_month = Variable.get(variable_key, default_var="0")

        # Pastikan integer dan validasi range
        try:
            last_processed_month = int(last_processed_month)
        except (ValueError, TypeError):
            logger.warning(f"Invalid last_processed_month value: {last_processed_month}. Defaulting to 0")
            last_processed_month = 0
        
        # Validasi range month (0-12)
        if last_processed_month < 0 or last_processed_month > 12:
            logger.warning(f"last_processed_month out of range: {last_processed_month}. Resetting to 0")
            last_processed_month = 0
        
        next_month = last_processed_month + 1
        
        # Handle year rollover
        if next_month > 12:
            logger.info(f"Completed year {target_year} for {table_name}, would need to move to next year")

            next_month = 1

        logger.info(f"[STATE] Table: {table_name}, Year: {target_year}, Last Month: {last_processed_month}, Next Month: {next_month}")
        
        # Final validation
        if next_month < 1 or next_month > 12:
            logger.error(f"Generated invalid next_month: {next_month}. Defaulting to 1")
            next_month = 1
            
        return next_month
    
    except Exception as e:
        logger.error(f"Error in get_next_month_to_process for {table_name}: {e}", exc_info=True)
        logger.info("Defaulting to month 1 due to error")
        return 1

def update_last_processed_month(table_name, target_year, month):
    """Update last processed month in Airflow Variable dengan validasi"""
    try:
        # Validasi input
        if not isinstance(month, int) or month < 1 or month > 12:
            logger.error(f"Invalid month value for update: {month}. Must be integer 1-12")
            return False
            
        variable_key = f"etl_last_month_{table_name}_{target_year}"
        Variable.set(variable_key, str(month))
        logger.info(f"✅ [STATE UPDATE] {variable_key} set to {month}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating last processed month for {table_name}: {e}", exc_info=True)
        return False
