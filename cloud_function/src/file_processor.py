import logging
import hashlib
import pandas as pd
import io
from typing import Dict, List, Any
from datetime import datetime
from google.cloud import storage

logger = logging.getLogger(__name__)

class FileProcessor:
    """Handle file processing from Google Cloud Storage"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
    
    def get_file_hash(self, file_path: str) -> str:
        """Generate hash of file content for deduplication"""
        try:
            blob = self.bucket.blob(file_path)
            file_content = blob.download_as_bytes()
            return hashlib.md5(file_content).hexdigest()
        except Exception as e:
            logger.error(f"Error getting file hash for {file_path}: {str(e)}")
            raise
    
    def parse_file(self, file_path: str, institution: str) -> Dict[str, Any]:
        """Parse CSV/XLSX file and return structured data"""
        logger.info(f"Parsing file: {file_path} for institution: {institution}")
        
        try:
            # Download file from GCS
            blob = self.bucket.blob(file_path)
            file_content = blob.download_as_bytes()
            file_hash = hashlib.md5(file_content).hexdigest()
            
            # Determine file type and parse
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_content))
            elif file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_content))
            else:
                raise ValueError(f"Unsupported file type: {file_path}")
            
            # Parse based on institution
            if institution == 'amex':
                parsed_data = self._parse_amex_data(df)
            elif institution == 'wealthsimple':
                parsed_data = self._parse_wealthsimple_data(df)
            else:
                raise ValueError(f"Unsupported institution: {institution}")
            
            # Add metadata to each row
            upload_timestamp = datetime.utcnow()
            rows_with_metadata = []
            
            for row in parsed_data:
                # Generate row hash for deduplication
                row_content = str(sorted(row.items()))
                row_hash = hashlib.md5(row_content.encode()).hexdigest()
                
                row_with_meta = {
                    **row,
                    'file_name': file_path,
                    'file_hash': file_hash,
                    'upload_timestamp': upload_timestamp,
                    'processed_timestamp': None,
                    'row_hash': row_hash,
                    'is_processed': False
                }
                rows_with_metadata.append(row_with_meta)
            
            return {
                "rows": rows_with_metadata,
                "metadata": {
                    "file_name": file_path,
                    "file_hash": file_hash,
                    "row_count": len(rows_with_metadata),
                    "upload_timestamp": upload_timestamp
                }
            }
            
        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {str(e)}", exc_info=True)
            raise
    
    def _parse_amex_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parse AmEx CSV data"""
        logger.info("Parsing AmEx data")
        
        # Expected columns from your sample
        expected_columns = [
            'Date', 'Date Processed', 'Description', 'Cardmember',
            'Amount', 'Foreign Spend Amount', 'Commission', 'Exchange Rate',
            'Merchant', 'Merchant Address', 'Additional Information'
        ]
        
        # Normalize column names (handle case variations)
        df.columns = df.columns.str.strip()
        
        # Validate required columns exist
        missing_columns = []
        for col in ['Date', 'Description', 'Amount']:
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            raise ValueError(f"Missing required AmEx columns: {missing_columns}")
        
        rows = []
        for _, row in df.iterrows():
            parsed_row = {
                'date': pd.to_datetime(row.get('Date')).date() if pd.notna(row.get('Date')) else None,
                'date_processed': pd.to_datetime(row.get('Date Processed')).date() if pd.notna(row.get('Date Processed')) else None,
                'description': str(row.get('Description', '')).strip(),
                'cardmember': str(row.get('Cardmember', '')).strip(),
                'amount': float(row.get('Amount', 0)) if pd.notna(row.get('Amount')) else 0.0,
                'foreign_spend_amount': float(row.get('Foreign Spend Amount', 0)) if pd.notna(row.get('Foreign Spend Amount')) else None,
                'commission': float(row.get('Commission', 0)) if pd.notna(row.get('Commission')) else None,
                'exchange_rate': float(row.get('Exchange Rate', 0)) if pd.notna(row.get('Exchange Rate')) else None,
                'merchant': str(row.get('Merchant', '')).strip() if pd.notna(row.get('Merchant')) else None,
                'merchant_address': str(row.get('Merchant Address', '')).strip() if pd.notna(row.get('Merchant Address')) else None,
                'additional_information': str(row.get('Additional Information', '')).strip() if pd.notna(row.get('Additional Information')) else None
            }
            rows.append(parsed_row)
        
        logger.info(f"Parsed {len(rows)} AmEx transactions")
        return rows
    
    def _parse_wealthsimple_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parse Wealthsimple CSV data"""
        logger.info("Parsing Wealthsimple data")
        
        # Expected columns from your sample
        expected_columns = ['date', 'transaction', 'description', 'amount', 'balance']
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.strip()
        
        # Validate required columns exist
        missing_columns = []
        for col in expected_columns:
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            raise ValueError(f"Missing required Wealthsimple columns: {missing_columns}")
        
        rows = []
        for _, row in df.iterrows():
            parsed_row = {
                'date': pd.to_datetime(row.get('date')).date() if pd.notna(row.get('date')) else None,
                'transaction': str(row.get('transaction', '')).strip(),
                'description': str(row.get('description', '')).strip(),
                'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0.0,
                'balance': float(row.get('balance', 0)) if pd.notna(row.get('balance')) else None
            }
            rows.append(parsed_row)
        
        logger.info(f"Parsed {len(rows)} Wealthsimple transactions")
        return rows 