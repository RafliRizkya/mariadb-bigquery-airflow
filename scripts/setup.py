import os
import json
import logging
import sys
from airflow import settings
from airflow.models import Connection, Variable
from airflow.configuration import conf

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_airflow_ready():
    """Check if Airflow is properly initialized"""
    try:
        # Test database connection
        session = settings.Session()
        session.execute("SELECT 1")
        session.close()
        logger.info("✅ Airflow database connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Airflow database not ready: {e}")
        return False

def setup_connections():
    """Setup Airflow connections untuk Docker environment"""
    logger.info("🔌 Setting up Airflow connections...")
    
    connections = [
        {
            'conn_id': 'mariadb_default',
            'conn_type': 'mysql',
            'host': os.environ.get("MARIADB_HOST", 'your-host'),
            'login': os.environ.get("MARIADB_USER", 'your-user'),
            'password': os.environ.get("MARIADB_PASSWORD", 'your-password'),
            'schema': os.environ.get("MARIADB_DATABASE", 'your-database'),
            'port': int(os.environ.get("MARIADB_PORT", 3306))
        },
        {
            'conn_id': 'google_cloud_default', 
            'conn_type': 'google_cloud_platform',
            'extra': json.dumps({
                "extra__google_cloud_platform__project": os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id"),
                "extra__google_cloud_platform__key_path": "/opt/airflow/credentials/service-account.json"
            })
        },
        {
            'conn_id': 'bigquery_default',
            'conn_type': 'google_cloud_platform',
            'extra': json.dumps({
                "extra__google_cloud_platform__project": os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id"),
                "extra__google_cloud_platform__key_path": "/opt/airflow/credentials/service-account.json",
                "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/bigquery"
            })
        }
    ]
    
    session = settings.Session()
    try:
        for conn_config in connections:
            conn_id = conn_config['conn_id']
            
            # Check if connection exists
            existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            
            if existing:
                logger.info(f"♻️  Updating existing connection: {conn_id}")
                # Update existing connection attributes
                for key, value in conn_config.items():
                    if key == 'extra' and hasattr(existing, 'extra'):
                        existing.extra = value
                    elif hasattr(existing, key) and key != 'conn_id':
                        setattr(existing, key, value)
                session.merge(existing)
            else:
                logger.info(f"🆕 Creating new connection: {conn_id}")
                conn = Connection(**conn_config)
                session.add(conn)
        
        session.commit()
        logger.info("✅ Connections setup completed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error setting up connections: {e}")
        session.rollback()
        return False
    finally:
        session.close()

def setup_variables():
    """Setup Airflow variables"""
    logger.info("📝 Setting up Airflow variables...")
    
    variables = {
        # BigQuery Configuration
        'GOOGLE_CLOUD_PROJECT': os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id"),
        'BIGQUERY_DATASET': os.environ.get("BIGQUERY_DATASET", "your-dataset"),
        
        # ETL Configuration
        'TARGET_YEAR': '2024',
        'BATCH_SIZE': '10000',
        'MAX_RETRIES': '3',
        'RETRY_DELAY': '300',  # 5 minutes in seconds
        
        # MariaDB Configuration
        'MARIADB_HOST': os.environ.get("MARIADB_HOST", "your-host"),
        'MARIADB_DATABASE': os.environ.get("MARIADB_DATABASE", "your-database"),
        'MARIADB_PORT': str(os.environ.get("MARIADB_PORT", "3306")),
        
        # ETL State Management - Initialize processing state for tables
        'etl_last_month_your_table_2024': '0',  # Start from month 1
        'etl_enabled_tables': json.dumps(['your-table-name']),  # List of enabled tables
        
        # Processing Configuration
        'ETL_CHUNK_SIZE': '1000',
        'ETL_TIMEOUT': '3600',  # 1 hour timeout
        'ETL_LOG_LEVEL': 'INFO',
        
        # Notification Settings (optional)
        'SLACK_WEBHOOK': os.environ.get("SLACK_WEBHOOK", ""),
        'EMAIL_NOTIFICATIONS': os.environ.get("EMAIL_NOTIFICATIONS", "false"),
    }
    
    try:
        success_count = 0
        for key, value in variables.items():
            if value is not None and str(value).strip():
                try:
                    Variable.set(key, str(value))
                    logger.info(f"✅ Set variable: {key} = {value}")
                    success_count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to set variable {key}: {e}")
            else:
                logger.warning(f"⚠️  Variable {key} is empty, skipping...")
                
        logger.info(f"✅ Variables setup completed! ({success_count}/{len(variables)} set)")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ Error setting up variables: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def setup_table_state_variables():
    """Setup table-specific state variables for monthly ETL"""
    logger.info("📊 Setting up table state variables...")
    
    # List of tables yang akan di-process
    tables = ['your-table']
    years = [2024]
    
    try:
        for table in tables:
            for year in years:
                variable_key = f"etl_last_month_{table}_{year}"
                
                # Check if variable already exists
                try:
                    existing_value = Variable.get(variable_key)
                    logger.info(f"📋 Existing state: {variable_key} = {existing_value}")
                except:
                    # Variable doesn't exist, create it
                    Variable.set(variable_key, "0")
                    logger.info(f"🆕 Created state variable: {variable_key} = 0")
                    
        logger.info("✅ Table state variables setup completed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error setting up table state variables: {e}")
        return False

def validate_setup():
    """Validate setup completion"""
    logger.info("🔍 Validating setup...")
    
    validation_results = {
        'connections': False,
        'variables': False,
        'table_states': False
    }
    
    session = settings.Session()
    try:
        # Check connections
        required_connections = ['mariadb_default', 'google_cloud_default']
        connections = session.query(Connection).filter(
            Connection.conn_id.in_(required_connections)
        ).all()
        
        found_connections = [conn.conn_id for conn in connections]
        logger.info(f"Found {len(connections)} connections: {found_connections}")
        
        for conn in connections:
            logger.info(f"✅ Connection: {conn.conn_id} ({conn.conn_type})")
            
        validation_results['connections'] = len(connections) >= 2
            
        # Check variables
        required_vars = [
            'GOOGLE_CLOUD_PROJECT', 
            'BIGQUERY_DATASET', 
            'TARGET_YEAR',
            'MARIADB_HOST',
            'MARIADB_DATABASE'
        ]
        
        var_count = 0
        for var_key in required_vars:
            try:
                value = Variable.get(var_key)
                logger.info(f"✅ Variable {var_key}: {value}")
                var_count += 1
            except Exception as e:
                logger.warning(f"⚠️  Variable {var_key}: Missing - {e}")
                
        validation_results['variables'] = var_count >= 4
        
        # Check table state variables
        try:
            state_var = Variable.get('etl_last_month_your_table_2024')
            logger.info(f"✅ Table state: etl_last_month_your_table_2024 = {state_var}")
            validation_results['table_states'] = True
        except:
            logger.warning("⚠️  Table state variables not found")
            
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
    finally:
        session.close()
    
    # Summary
    total_checks = len(validation_results)
    passed_checks = sum(validation_results.values())
    
    logger.info(f"\n📋 Validation Summary: {passed_checks}/{total_checks} checks passed")
    for check, result in validation_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"  {check}: {status}")
    
    return passed_checks == total_checks

def test_connections():
    """Test database connections"""
    logger.info("🧪 Testing connections...")
    
    # Test MariaDB connection
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=os.environ.get("MARIADB_HOST", "your-host"),
            port=int(os.environ.get("MARIADB_PORT", 3306)),
            user=os.environ.get("MARIADB_USER", "your-user"),
            password=os.environ.get("MARIADB_PASSWORD", "your-password"),
            database=os.environ.get("MARIADB_DATABASE", "your-database")
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        logger.info("✅ MariaDB connection test successful")
        
    except Exception as e:
        logger.error(f"❌ MariaDB connection test failed: {e}")
    
    # Test Google Cloud connection (simple check)
    try:
        service_account_path = "/opt/airflow/credentials/service-account.json"
        if os.path.exists(service_account_path):
            logger.info("✅ Google Cloud service account file found")
        else:
            logger.warning("⚠️  Google Cloud service account file not found")
            
    except Exception as e:
        logger.error(f"❌ Google Cloud connection test failed: {e}")

def main():
    """Main setup function"""
    try:
        logger.info("🚀 Starting Airflow setup...")
        logger.info(f"Python version: {sys.version}")
        
        # Check if Airflow is ready
        if not check_airflow_ready():
            logger.error("❌ Airflow is not ready. Please wait for Airflow to fully start.")
            sys.exit(1)
        
        # Setup connections and variables
        logger.info("\n" + "="*50)
        connections_ok = setup_connections()
        
        logger.info("\n" + "="*50)
        variables_ok = setup_variables()
        
        logger.info("\n" + "="*50)
        table_states_ok = setup_table_state_variables()
        
        # Test connections
        logger.info("\n" + "="*50)
        test_connections()
        
        # Validate setup
        logger.info("\n" + "="*50)
        validation_ok = validate_setup()
        
        if connections_ok and variables_ok and table_states_ok and validation_ok:
            logger.info("\n🎉 Setup completed successfully!")
            logger.info("\n📋 Next steps:")
            logger.info("1. Check Airflow UI (http://localhost:8080)")
            logger.info("2. Go to Admin → Connections to verify")
            logger.info("3. Go to Admin → Variables to verify") 
            logger.info("4. Enable daily_etl_dag")
            logger.info("5. Test run the DAG")
            logger.info("\n🔍 Monitoring:")
            logger.info("- Check ETL progress via Variables (etl_last_month_*)")
            logger.info("- Monitor DAG runs in Airflow UI")
            logger.info("- Check BigQuery dataset for results")
        else:
            logger.error("❌ Setup completed with some issues. Please check the logs above.")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    # Can be run with arguments for specific operations
    if len(sys.argv) > 1:
        if sys.argv[1] == "--validate":
            check_airflow_ready() and validate_setup()
        elif sys.argv[1] == "--test":
            test_connections()
        else:
            main()
    else:
        main()
