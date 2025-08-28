from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, timezone
import sys
import os
import logging

# Setup logging at module level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup path - Fix path issues
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_HOME = os.path.abspath(os.path.join(BASE_DIR, ".."))
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)  # Use insert instead of append for priority

try:
    from src.operators.mariadb_to_bigquery_operator import MariaDBToBigQueryOperator
    from src.utils.config_loader import load_table_config, get_next_month_to_process, update_last_processed_month
    logger.info("✅ Successfully imported custom modules")
except ImportError as e:
    logger.error(f"❌ Import error: {e}")
    logger.error(f"Python path: {sys.path}")
    raise

def run_daily_etl(table_name, **context):
    """
    Actual ETL implementation untuk daily processing
    """
    try:
        logger.info(f"🚀 Starting daily ETL for table: {table_name}")
        logger.info(f"📅 Execution date: {context.get('execution_date')}")
        logger.info(f"📋 Context keys: {list(context.keys())}")
        
        # Load table config inside the task (not at DAG level)
        table_configs, _ = load_table_config()
        logger.info(f"📊 Loaded configs for tables: {list(table_configs.keys())}")
        
        if table_name not in table_configs:
            available_tables = list(table_configs.keys())
            raise Exception(f"Table {table_name} not found in config. Available: {available_tables}")
            
        config = table_configs[table_name]
        logger.info(f"📋 Config for {table_name}: {config}")
        
        # Get target year from params or default
        target_year = int(context.get('params', {}).get('target_year', 2024))
        logger.info(f"🎯 Target year: {target_year}")
        
        # Create and execute MariaDBToBigQueryOperator
        operator = MariaDBToBigQueryOperator(
            task_id=f"etl_{table_name}_operator",
            source_table=config.get('source_table', table_name),
            date_column=config.get('date_column', 'creation'),
            order_by=config.get('order_by', 'creation'),
            extract_type=config.get('extract_type', 'monthly'),
            destination_table=config.get('destination_table', table_name),
            target_year=target_year,
            month=None  # Let operator auto-detect month
        )
        
        result = operator.execute(context)
        logger.info(f"✅ ETL completed for {table_name}: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ ETL failed for {table_name}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def get_processable_tables():
    """Get list of tables that can be processed - called as a separate function"""
    try:
        table_configs, _ = load_table_config()
        
        if not table_configs:
            logger.warning("⚠️ No table configs found")
            return []
            
        # Filter table yang perlu diproses
        processable_tables = []
        for table, config in table_configs.items():
            extract_type = config.get("extract_type", "monthly")
            if extract_type in ["daily", "monthly"]:
                processable_tables.append(table)
                logger.info(f"✅ Added processable table: {table} (type: {extract_type})")
            else:
                logger.info(f"⏭️ Skipped table: {table} (type: {extract_type})")
                
        logger.info(f"📊 Total processable tables: {len(processable_tables)}")
        return processable_tables
        
    except Exception as e:
        logger.error(f"❌ Error getting processable tables: {e}")
        return []
    
WIB = timezone(timedelta(hours=7))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 8, 27, 11, 0, tzinfo=WIB), #example
    "catchup": False,
}

# Create DAG
dag = DAG(
    "your_dag_id",
    default_args=default_args,
    description="enter description dag here",
    #example using cron job
    schedule_interval="0 11 * * *",
    catchup=False,
    tags=["etl", "daily"],
    params={
        "target_year": 2024 #example
    },
    max_active_runs=1,  # Prevent multiple runs at same time
    max_active_tasks=3, 
)

# Define tasks within DAG context
with dag:
    
    start_task = EmptyOperator(
        task_id="start_daily_etl",
        dag=dag
    )
    
    end_task = EmptyOperator(
        task_id="end_daily_etl", 
        dag=dag
    )
    
    # Get processable tables - do this at DAG parse time but handle errors gracefully
    try:
        daily_processable_tables = get_processable_tables()
        logger.info(f"[DAG INIT] Found processable tables: {daily_processable_tables}")
    except Exception as e:
        logger.error(f"[DAG INIT] Error loading tables: {e}")
        daily_processable_tables = ["your-table-name"]  # Fallback to known table
    
    if daily_processable_tables:
        etl_tasks = []
        
        for table_name in daily_processable_tables:
            # Create individual ETL task for each table
            etl_task = PythonOperator(
                task_id=f"etl_{table_name}",
                python_callable=run_daily_etl,
                op_kwargs={"table_name": table_name},
                provide_context=True,
                dag=dag,
                pool='default_pool',  # Use default pool
                retries=1,  # Override default retries for individual tasks
                retry_delay=timedelta(minutes=3)
            )
            etl_tasks.append(etl_task)
            logger.info(f"🔧 Created task: etl_{table_name}")
        
        # Set dependencies - all ETL tasks run in parallel after start
        start_task >> etl_tasks >> end_task
        
        logger.info(f"🔗 Created dependencies: start → {len(etl_tasks)} ETL tasks → end")
        
    else:
        # Fallback task when no tables found
        no_tables_task = EmptyOperator(
            task_id="no_tables_found",
            dag=dag
        )
        start_task >> no_tables_task >> end_task
        logger.warning("⚠️ No processable tables found, created fallback task")

# Log final DAG structure
logger.info(f"📋 DAG '{dag.dag_id}' created with {len(dag.task_dict)} tasks")
logger.info(f"📋 Tasks: {list(dag.task_dict.keys())}")