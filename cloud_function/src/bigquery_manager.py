import logging
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime

logger = logging.getLogger(__name__)

class BigQueryManager:
    """Handle all BigQuery operations"""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id)
        
        # Ensure dataset and tables exist
        self._ensure_dataset_exists()
        self._ensure_tables_exist()
    
    def _ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def _ensure_tables_exist(self):
        """Create all required tables if they don't exist"""
        tables_to_create = {
            'raw_amex_transactions': self._get_raw_amex_schema(),
            'raw_wealthsimple_transactions': self._get_raw_wealthsimple_schema(),
            'dim_categories': self._get_dim_categories_schema(),
            'dim_description_categories': self._get_dim_description_categories_schema()
        }
        
        for table_name, schema in tables_to_create.items():
            self._create_table_if_not_exists(table_name, schema)
    
    def _create_table_if_not_exists(self, table_name: str, schema: List[bigquery.SchemaField]):
        """Create table if it doesn't exist"""
        table_ref = self.dataset_ref.table(table_name)
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_name} already exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {table_name}")
    
    def _get_raw_amex_schema(self) -> List[bigquery.SchemaField]:
        """Schema for raw_amex_transactions table"""
        return [
            bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("upload_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("date_processed", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cardmember", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("foreign_spend_amount", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("commission", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("exchange_rate", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("merchant", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merchant_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("additional_information", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_processed", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
        ]
    
    def _get_raw_wealthsimple_schema(self) -> List[bigquery.SchemaField]:
        """Schema for raw_wealthsimple_transactions table"""
        return [
            bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("upload_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("transaction", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("balance", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("row_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_processed", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
        ]
    
    def _get_dim_categories_schema(self) -> List[bigquery.SchemaField]:
        """Schema for dim_categories table"""
        return [
            bigquery.SchemaField("category_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("general_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("detailed_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
        ]
    
    def _get_dim_description_categories_schema(self) -> List[bigquery.SchemaField]:
        """Schema for dim_description_categories table"""
        return [
            bigquery.SchemaField("description_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("original_description", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("general_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("detailed_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("confidence_score", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("gemini_model_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED")
        ]
    
    def file_already_processed(self, file_hash: str) -> bool:
        """Check if file has already been processed"""
        query = f"""
        SELECT COUNT(*) as count
        FROM (
            SELECT file_hash FROM `{self.project_id}.{self.dataset_id}.raw_amex_transactions`
            WHERE file_hash = @file_hash
            UNION ALL
            SELECT file_hash FROM `{self.project_id}.{self.dataset_id}.raw_wealthsimple_transactions`
            WHERE file_hash = @file_hash
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
            ]
        )
        
        result = self.client.query(query, job_config=job_config).result()
        count = next(result).count
        return count > 0
    
    def load_raw_data(self, institution: str, rows: List[Dict[str, Any]], metadata: Dict[str, Any]) -> int:
        """Load raw data into appropriate BigQuery table"""
        table_name = f"raw_{institution}_transactions"
        table_ref = self.dataset_ref.table(table_name)
        
        # Convert datetime objects to strings for BigQuery
        processed_rows = []
        for row in rows:
            processed_row = row.copy()
            for key, value in processed_row.items():
                if isinstance(value, datetime):
                    processed_row[key] = value.isoformat()
                elif hasattr(value, 'isoformat'):  # date objects
                    processed_row[key] = value.isoformat()
            
            # Add created_at timestamp
            processed_row['created_at'] = datetime.utcnow().isoformat()
            processed_rows.append(processed_row)
        
        # Load data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_json(
            processed_rows, table_ref, job_config=job_config
        )
        job.result()  # Wait for job to complete
        
        logger.info(f"Loaded {len(processed_rows)} rows into {table_name}")
        return len(processed_rows)
    
    def get_uncategorized_descriptions(self) -> List[str]:
        """Get unique descriptions that haven't been categorized by Gemini"""
        query = f"""
        WITH all_descriptions AS (
            SELECT DISTINCT UPPER(TRIM(description)) as description_key
            FROM (
                SELECT description FROM `{self.project_id}.{self.dataset_id}.raw_amex_transactions`
                WHERE description IS NOT NULL AND description != ''
                UNION ALL
                SELECT description FROM `{self.project_id}.{self.dataset_id}.raw_wealthsimple_transactions`
                WHERE description IS NOT NULL AND description != ''
            )
        )
        SELECT ad.description_key
        FROM all_descriptions ad
        LEFT JOIN `{self.project_id}.{self.dataset_id}.dim_description_categories` dc
            ON ad.description_key = UPPER(TRIM(dc.description_key))
        WHERE dc.description_key IS NULL
        ORDER BY ad.description_key
        LIMIT 100  -- Process in batches to avoid Gemini API limits
        """
        
        result = self.client.query(query).result()
        descriptions = [row.description_key for row in result]
        
        logger.info(f"Found {len(descriptions)} uncategorized descriptions")
        return descriptions
    
    def update_category_cache(self, new_categories: List[Dict[str, Any]]) -> int:
        """Update the category cache with new Gemini classifications"""
        if not new_categories:
            return 0
        
        table_ref = self.dataset_ref.table('dim_description_categories')
        
        # Prepare data for insertion
        rows_to_insert = []
        for category in new_categories:
            row = {
                'description_key': category['description_key'],
                'original_description': category['original_description'],
                'general_category': category['general_category'],
                'detailed_category': category['detailed_category'],
                'confidence_score': category.get('confidence_score'),
                'gemini_model_version': category.get('gemini_model_version', 'gemini-1.5-flash'),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            rows_to_insert.append(row)
        
        # Insert data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        job.result()  # Wait for job to complete
        
        logger.info(f"Updated category cache with {len(rows_to_insert)} new entries")
        return len(rows_to_insert)
    
    def delete_file_data(self, file_hash: str) -> int:
        """Delete all data associated with a file hash"""
        total_deleted = 0
        
        # Delete from both raw tables
        tables = ['raw_amex_transactions', 'raw_wealthsimple_transactions']
        
        for table_name in tables:
            query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            WHERE file_hash = @file_hash
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
                ]
            )
            
            job = self.client.query(query, job_config=job_config)
            result = job.result()
            
            if hasattr(job, 'num_dml_affected_rows'):
                rows_deleted = job.num_dml_affected_rows or 0
                total_deleted += rows_deleted
                logger.info(f"Deleted {rows_deleted} rows from {table_name}")
        
        return total_deleted
    
    def initialize_categories(self, categories_data: List[Dict[str, str]]):
        """Initialize the categories dimension table with predefined data"""
        table_ref = self.dataset_ref.table('dim_categories')
        
        # Check if data already exists
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.dim_categories`"
        result = self.client.query(query).result()
        count = next(result).count
        
        if count > 0:
            logger.info("Categories table already populated")
            return
        
        # Prepare data for insertion
        rows_to_insert = []
        for i, category in enumerate(categories_data):
            row = {
                'category_id': f"cat_{i+1:03d}",
                'general_category': category['general_category'],
                'detailed_category': category['detailed_category'],
                'is_active': True,
                'created_at': datetime.utcnow().isoformat()
            }
            rows_to_insert.append(row)
        
        # Insert data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        job.result()  # Wait for job to complete
        
        logger.info(f"Initialized categories table with {len(rows_to_insert)} categories") 