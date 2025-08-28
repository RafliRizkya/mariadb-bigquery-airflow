import os
import pandas as pd
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import mysql.connector
import logging
import datetime as dt

class MariaDBToBigQueryOperator(BaseOperator):
    
    def __init__(
        self,
        source_table: str,
        date_column: str,
        order_by: str,
        extract_type: str = "monthly",
        destination_table: str = None,
        target_year: int = 2024,
        month: int = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        
        self.source_table = source_table
        self.date_column = date_column
        self.order_by = order_by
        self.extract_type = extract_type
        self.destination_table = destination_table or source_table
        self.target_year = target_year
        self.month = month
        
        # Environment variables
        self.project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        self.dataset = os.environ.get("BIGQUERY_DATASET")
        
        # MariaDB connection parameters
        self.mariadb_host = os.environ.get("MARIADB_HOST")
        self.mariadb_port = os.environ.get("MARIADB_PORT")
        self.mariadb_user = os.environ.get("MARIADB_USER")
        self.mariadb_password = os.environ.get("MARIADB_PASSWORD")
        self.mariadb_database = os.environ.get("MARIADB_DATABASE")

    def execute(self, context):
        try:
            # Import config_loader untuk mendapatkan month yang tepat
            from src.utils.config_loader import get_next_month_to_process, update_last_processed_month
            
            # Jika month tidak diberikan dan extract_type adalah monthly, ambil dari state
            if self.extract_type == "monthly" and self.month is None:
                self.month = get_next_month_to_process(self.source_table, self.target_year)
                self.log.info(f"🔄 Auto-detected next month to process: {self.month}")
            
            self.log.info("🔌 Connecting to MariaDB directly...")
            
            # Step 1: Connect to MariaDB dengan mysql.connector
            conn = mysql.connector.connect(
                host=self.mariadb_host,
                port=self.mariadb_port,
                user=self.mariadb_user,
                password=self.mariadb_password,
                database=self.mariadb_database
            )
            
            self.log.info("✅ MariaDB connection successful")
            
            # Step 2: Generate and execute SQL query
            sql_query = self._get_extract_sql(context)
            self.log.info(f"📝 Executing SQL: {sql_query}")
            
            # Extract data dengan pandas
            df = pd.read_sql(sql_query, conn)
            conn.close()

            # DEBUG: Cek kolom yang punya float values tapi mungkin harus integer
            for col in df.columns:
                if df[col].dtype == 'float64':
                    float_vals = df[col][df[col] % 1 != 0]
                    if not float_vals.empty:
                        self.log.warning(f"FOUND FLOAT VALUES in {col}: {float_vals.unique()[:5]}")  # Show first 5
            
            self.log.info(f"📊 Extracted {len(df)} records from {self.source_table}")
            
            if df.empty:
                self.log.warning(f"⚠️ No data found for {self.source_table} month {self.month}")
                
                # Jika monthly dan tidak ada data, tetap update state untuk skip month ini
                if self.extract_type == "monthly" and self.month:
                    update_last_processed_month(self.source_table, self.target_year, self.month)
                    self.log.info(f"📝 Updated state: skipped empty month {self.month}")
                
                return f"No data extracted for {self.source_table} month {self.month}"
            
            # Step 3: Transform data
            self.log.info("🔄 Transforming data...")
            df_transformed = self._transform_data(df, context)
            
            # Step 4: Load to BigQuery
            self.log.info("⬆️ Loading to BigQuery...")
            self._load_to_bigquery(df_transformed)
            
            # Step 5: Update state jika berhasil dan monthly
            if self.extract_type == "monthly" and self.month:
                update_last_processed_month(self.source_table, self.target_year, self.month)
                self.log.info(f"📝 Updated state: completed month {self.month}")
            
            self.log.info(f"✅ ETL completed successfully: {len(df_transformed)} records processed")
            return f"Successfully processed {len(df_transformed)} records"
            
        except Exception as e:
            self.log.error(f"❌ ETL failed: {str(e)}")
            import traceback
            self.log.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _get_extract_sql(self, context):
        """Generate SQL query based on extract type dengan validasi parameter"""
        
        if self.extract_type == "monthly":
            # Validasi month parameter
            if self.month is None:
                raise AirflowException("Month parameter is required for monthly extract")
            
            if not isinstance(self.month, int) or self.month < 1 or self.month > 12:
                raise AirflowException(f"Invalid month value: {self.month}. Must be integer between 1-12")
            
            return f"""
                SELECT * 
                FROM {self.source_table}
                WHERE MONTH({self.date_column}) = {self.month}
                AND YEAR({self.date_column}) = {self.target_year}
                ORDER BY {self.order_by}
            """
            
        elif self.extract_type == "daily":
            # Extract untuk hari tertentu berdasarkan execution_date
            execution_date = context.get('execution_date')
            if not execution_date:
                raise AirflowException("execution_date is required for daily extract")
            
            target_date = execution_date.strftime('%Y-%m-%d')
            return f"""
                SELECT * 
                FROM {self.source_table}
                WHERE DATE({self.date_column}) = '{target_date}'
                ORDER BY {self.order_by} 
            """
            
        elif self.extract_type == "incremental":
            # Extract berdasarkan range tanggal
            prev_run = context.get('prev_data_interval_end')
            current_run = context.get('data_interval_end')
            
            if not prev_run or not current_run:
                # Fallback ke execution date untuk backward compatibility
                execution_date = context.get('execution_date')
                if execution_date:
                    prev_run = execution_date.replace(hour=0, minute=0, second=0)
                    current_run = execution_date.replace(hour=23, minute=59, second=59)
                else:
                    raise AirflowException("Cannot determine date range for incremental extract")
            
            return f"""
                SELECT * 
                FROM {self.source_table}
                WHERE {self.date_column} >= '{prev_run.strftime('%Y-%m-%d %H:%M:%S')}'
                AND {self.date_column} < '{current_run.strftime('%Y-%m-%d %H:%M:%S')}'
                ORDER BY {self.order_by} 
            """
            
        elif self.extract_type == "full":
            return f"""
                SELECT * 
                FROM {self.source_table}
                ORDER BY {self.order_by} 
            """
        else:
            raise AirflowException(f"Unsupported extract_type: {self.extract_type}")

    def _transform_data(self, df, context):
        """Transform data dengan exact column names match"""
        self.log.info("Transforming data for exact BigQuery schema match")
        
        try:
            # 1. Convert semua column names ke lowercase (atau sesuai schema BigQuery)
            df.columns = df.columns.str.lower()
            
            # 2. Handle datetime dan date columns berdasarkan BigQuery schema
            datetime_columns = ['creation', 'modified']  # type: datetime
            date_columns = ['transaction_date', 'valid_till']  # type: date
            
            for col in datetime_columns:
                if col in df.columns:
                    # Convert to datetime and handle timezone
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            for col in date_columns:
                if col in df.columns:
                    # Convert to datetime first, then extract date
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
            # 3. Handle numeric columns - FIXED: Check for float values first
            integer_columns = ['docstatus', 'idx', 'total_qty']  # Columns that should be integers
            float_columns = []  # Add any columns that should remain as floats

            # Handle integer columns (if any)
            for col in integer_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Always round float values before converting to int (safer approach)
                    df[col] = df[col].round().fillna(0).astype(int)
                    self.log.info(f"Converted {col} to integer")
            
            # Handle float columns (if any)
            for col in float_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
            # 4. Handle string columns - pastikan tidak null untuk REQUIRED fields
            string_columns = ['name']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).replace('nan', '').fillna('')
            
            # 5. Remove any completely null rows
            df = df.dropna(how='all')
            
            # 6. Log data types for debugging
            self.log.info(f"Final DataFrame dtypes: {dict(df.dtypes)}")
            
            self.log.info(f"Data transformation completed: {len(df)} records ready")
            return df
            
        except Exception as e:
            self.log.error(f"Data transformation failed: {str(e)}")
            import traceback
            self.log.error(f"Transformation traceback: {traceback.format_exc()}")
            raise

    def _load_to_bigquery(self, df):
        """Load data dengan explicit schema validation"""
        table_id = f"{self.project_id}.{self.dataset}.{self.destination_table}"
        self.log.info(f"Loading {len(df)} records to {table_id} with schema validation")
        
        try:
            # Validate required environment variables
            if not self.project_id or not self.dataset:
                raise AirflowException("Missing required BigQuery configuration: GOOGLE_CLOUD_PROJECT or BIGQUERY_DATASET")
            
            # Pastikan column names exact match
            from google.cloud import bigquery
            
            # Get BigQuery table schema
            hook = BigQueryHook(gcp_conn_id='google_cloud_default')
            credentials = hook.get_credentials()
            client = bigquery.Client(credentials=credentials, project=self.project_id)
            
            # Check if table exists
            try:
                table_ref = client.dataset(self.dataset).table(self.destination_table)
                table = client.get_table(table_ref)
                
                # Validate columns match
                bq_columns = {field.name.lower() for field in table.schema}
                df_columns = set(df.columns)
                
                missing_in_df = bq_columns - df_columns
                missing_in_bq = df_columns - bq_columns
                
                if missing_in_df:
                    self.log.warning(f"Columns in BQ but not in DF: {missing_in_df}")
                    for col in missing_in_df:
                        df[col] = None  # Add missing columns with None
                
                if missing_in_bq:
                    self.log.warning(f"Columns in DF but not in BQ: {missing_in_bq}")
                    df = df.drop(columns=list(missing_in_bq))  # Remove extra columns
                    
            except Exception as e:
                self.log.warning(f"Could not validate schema (table might not exist): {e}")
            
            # Load data using pandas-gbq
            df.to_gbq(
                destination_table=f"{self.dataset}.{self.destination_table}",
                project_id=self.project_id,
                if_exists='append',
                progress_bar=False,
                chunksize=10000  # Process in chunks untuk large datasets
            )
            
            self.log.info(f"✅ Successfully loaded {len(df)} records to BigQuery")
            
        except Exception as e:
            self.log.error(f"Failed to load data to BigQuery: {str(e)}")
            import traceback
            self.log.error(f"BigQuery load traceback: {traceback.format_exc()}")
            raise