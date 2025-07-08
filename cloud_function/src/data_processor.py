import logging
import subprocess
import os
import hashlib
from typing import List, Dict, Any
from datetime import datetime

from .file_processor import FileProcessor
from .bigquery_manager import BigQueryManager
from .gemini_enricher import GeminiEnricher

logger = logging.getLogger(__name__)

class DataProcessor:
    """Main data processing orchestrator"""
    
    def __init__(self, project_id: str, dataset_id: str, bucket_name: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        
        # Initialize components
        self.file_processor = FileProcessor(bucket_name)
        self.bq_manager = BigQueryManager(project_id, dataset_id)
        self.gemini_enricher = GeminiEnricher(project_id)
        
        # Set dbt environment variables
        os.environ['DBT_GCP_PROJECT'] = project_id
    
    def process_files(self, institution: str, file_paths: List[str], force_reprocess: bool = False) -> Dict[str, Any]:
        """
        Main processing pipeline
        
        Steps:
        1. Check which files need processing (file hash cache)
        2. Parse CSV/XLSX files and load raw data to BigQuery
        3. Run dbt transformations
        4. Identify uncategorized transactions for Gemini enrichment
        5. Call Gemini for new descriptions
        6. Update category cache and re-run dbt
        """
        logger.info(f"Starting data processing for {institution} with {len(file_paths)} files")
        
        result = {
            "files_processed": 0,
            "rows_inserted": 0,
            "new_categories": 0,
            "dbt_models_built": 0,
            "processing_time_seconds": 0
        }
        
        start_time = datetime.now()
        
        try:
            # Step 1: Filter files that need processing
            files_to_process = self._filter_files_for_processing(file_paths, force_reprocess)
            logger.info(f"Files to process: {len(files_to_process)} of {len(file_paths)}")
            
            if not files_to_process:
                logger.info("No new files to process")
                return result
            
            # Step 2: Process files and load raw data
            rows_inserted = self._process_and_load_files(institution, files_to_process)
            result["files_processed"] = len(files_to_process)
            result["rows_inserted"] = rows_inserted
            
            # Step 3: Run initial dbt transformations (up to staging)
            self._run_dbt_models(["staging"])
            
            # Step 4: Find uncategorized transactions for Gemini
            uncategorized_descriptions = self.bq_manager.get_uncategorized_descriptions()
            logger.info(f"Found {len(uncategorized_descriptions)} uncategorized descriptions")
            
            # Step 5: Enrich with Gemini
            if uncategorized_descriptions:
                new_categories = self.gemini_enricher.categorize_descriptions(uncategorized_descriptions)
                self.bq_manager.update_category_cache(new_categories)
                result["new_categories"] = len(new_categories)
            
            # Step 6: Run final dbt transformations (all models)
            dbt_result = self._run_dbt_models(["intermediate", "marts"])
            result["dbt_models_built"] = dbt_result.get("models_built", 0)
            
            # Calculate processing time
            end_time = datetime.now()
            result["processing_time_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info(f"Processing completed successfully: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in data processing pipeline: {str(e)}", exc_info=True)
            raise
    
    def _filter_files_for_processing(self, file_paths: List[str], force_reprocess: bool) -> List[str]:
        """Filter files based on file hash cache"""
        if force_reprocess:
            return file_paths
        
        files_to_process = []
        for file_path in file_paths:
            file_hash = self.file_processor.get_file_hash(file_path)
            if not self.bq_manager.file_already_processed(file_hash):
                files_to_process.append(file_path)
        
        return files_to_process
    
    def _process_and_load_files(self, institution: str, file_paths: List[str]) -> int:
        """Process files and load data into BigQuery"""
        total_rows = 0
        
        for file_path in file_paths:
            logger.info(f"Processing file: {file_path}")
            
            # Parse file
            file_data = self.file_processor.parse_file(file_path, institution)
            
            # Load to BigQuery raw table
            rows_inserted = self.bq_manager.load_raw_data(
                institution, 
                file_data["rows"], 
                file_data["metadata"]
            )
            
            total_rows += rows_inserted
            logger.info(f"Loaded {rows_inserted} rows from {file_path}")
        
        return total_rows
    
    def _run_dbt_models(self, model_selections: List[str]) -> Dict[str, Any]:
        """Run dbt transformations"""
        logger.info(f"Running dbt models: {model_selections}")
        
        try:
            # Change to dbt project directory
            dbt_dir = "/app/dbt_project"
            
            # Install dbt packages if not already done
            subprocess.run(["dbt", "deps"], cwd=dbt_dir, check=True, capture_output=True)
            
            models_built = 0
            for selection in model_selections:
                # Run specific model selection
                cmd = ["dbt", "run", "--select", f"tag:{selection}"]
                result = subprocess.run(cmd, cwd=dbt_dir, check=True, capture_output=True, text=True)
                
                # Parse dbt output to count models built
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if "Completed successfully" in line and "model" in line:
                        models_built += 1
                
                logger.info(f"dbt run completed for {selection}: {result.stdout}")
            
            return {"models_built": models_built}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt run failed: {e.stderr}")
            raise Exception(f"dbt transformation failed: {e.stderr}")
    
    def delete_file_data(self, file_path: str) -> Dict[str, Any]:
        """Delete all data associated with a file"""
        logger.info(f"Deleting data for file: {file_path}")
        
        try:
            # Get file hash
            file_hash = self.file_processor.get_file_hash(file_path)
            
            # Delete from raw tables
            rows_deleted = self.bq_manager.delete_file_data(file_hash)
            
            # Re-run dbt to update downstream tables
            self._run_dbt_models(["staging", "intermediate", "marts"])
            
            return {
                "file_path": file_path,
                "rows_deleted": rows_deleted,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error deleting file data: {str(e)}", exc_info=True)
            raise 