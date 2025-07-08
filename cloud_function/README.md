# Money Mirror - Cloud Run Data Processor

This Cloud Run service handles the complete data processing pipeline for Money Mirror financial analytics.

## 🏗️ Architecture Overview

```
Streamlit App → HTTP Request → Cloud Run Function
                                      ↓
                          1. Parse CSV/XLSX files from GCS
                                      ↓
                          2. Load raw data to BigQuery
                                      ↓
                          3. Run dbt transformations
                                      ↓
                          4. Identify uncategorized transactions
                                      ↓
                          5. Call Gemini for AI categorization
                                      ↓
                          6. Update category cache in BigQuery
                                      ↓
                          7. Run final dbt models
```

## 📦 Components

- **`main.py`**: Flask application with REST API endpoints
- **`src/data_processor.py`**: Main orchestrator for the data pipeline
- **`src/file_processor.py`**: CSV/XLSX file parsing from Google Cloud Storage
- **`src/bigquery_manager.py`**: All BigQuery operations (DDL, DML, queries)
- **`src/gemini_enricher.py`**: Transaction categorization using Gemini AI
- **`dbt_project/`**: dbt models for data transformations

## 🚀 Deployment

### Prerequisites

1. **Google Cloud Project** with enabled APIs:

   ```bash
   gcloud services enable run.googleapis.com
   gcloud services enable cloudbuild.googleapis.com
   gcloud services enable secretmanager.googleapis.com
   gcloud services enable bigquery.googleapis.com
   ```

2. **Environment Variables**:

   ```bash
   export GCP_PROJECT_ID="your-project-id"
   ```

3. **Secrets in Google Secret Manager**:
   ```bash
   # Add your Gemini API key
   gcloud secrets create gemini-api-key --data-file=- <<< "your-gemini-api-key"
   ```

### Deploy to Cloud Run

```bash
cd cloud_function
./deploy.sh
```

The script will:

- Build Docker image
- Push to Google Container Registry
- Deploy to Cloud Run
- Output the service URL

### Manual Deployment

```bash
# Build image
docker build -t gcr.io/$GCP_PROJECT_ID/money-mirror-processor .

# Push image
docker push gcr.io/$GCP_PROJECT_ID/money-mirror-processor

# Deploy to Cloud Run
gcloud run deploy money-mirror-processor \
    --image gcr.io/$GCP_PROJECT_ID/money-mirror-processor \
    --platform managed \
    --region us-central1 \
    --memory 2Gi \
    --timeout 900 \
    --set-env-vars "DBT_GCP_PROJECT=$GCP_PROJECT_ID" \
    --allow-unauthenticated
```

## 🔧 Configuration

### Streamlit App Configuration

Add to your `secrets.toml`:

```toml
[cloud_run]
process_data_url = "https://your-service-url"
```

### One-time Setup

Initialize the categories table:

```bash
curl -X POST https://your-service-url/init-categories
```

## 📡 API Endpoints

### `POST /process-data`

Main data processing endpoint.

**Request:**

```json
{
  "institution": "amex" | "wealthsimple",
  "file_paths": ["path1", "path2"],
  "force_reprocess": false,
  "auth_token": "jwt_token"
}
```

**Response:**

```json
{
  "status": "success",
  "result": {
    "files_processed": 3,
    "rows_inserted": 450,
    "new_categories": 12,
    "dbt_models_built": 8,
    "processing_time_seconds": 45.2
  }
}
```

### `GET /health`

Health check endpoint.

### `POST /init-categories`

Initialize the categories dimension table with predefined taxonomy.

## 🧪 Local Testing

### Using Docker

```bash
# Build image
docker build -t money-mirror-processor .

# Run locally
docker run -p 8080:8080 \
    -e GCP_PROJECT_ID=your-project-id \
    -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json \
    -v /path/to/service-account.json:/path/to/service-account.json \
    money-mirror-processor
```

### Using Python

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GCP_PROJECT_ID=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Run the app
python main.py
```

### Test the API

```bash
# Health check
curl http://localhost:8080/health

# Initialize categories
curl -X POST http://localhost:8080/init-categories

# Test data processing
curl -X POST http://localhost:8080/process-data \
  -H "Content-Type: application/json" \
  -d '{
    "institution": "wealthsimple",
    "file_paths": ["wealthsimple/20241201-statement.csv"],
    "force_reprocess": false,
    "auth_token": "test"
  }'
```

## 🔄 Data Flow Details

### 1. File Processing

- Downloads files from Google Cloud Storage
- Generates file and row hashes for deduplication
- Parses CSV/XLSX based on institution format
- Validates data integrity

### 2. BigQuery Loading

- Creates tables if they don't exist
- Loads raw data with metadata
- Implements deduplication using row hashes

### 3. dbt Transformations

- Runs staging models to clean and standardize data
- Unions data from all institutions
- Applies business logic in intermediate models

### 4. AI Enrichment

- Identifies uncategorized transaction descriptions
- Batches requests to Gemini API (20 descriptions per batch)
- Caches results to prevent reprocessing
- Uses predefined category taxonomy

### 5. Final Processing

- Updates category cache in BigQuery
- Re-runs dbt models to incorporate new categories
- Builds final dashboard-ready tables

## 🚨 Error Handling

- **File parsing errors**: Returns specific error messages for malformed files
- **BigQuery errors**: Handles table creation, permission, and query errors
- **Gemini API errors**: Falls back to "Uncategorized" for failed requests
- **dbt errors**: Captures and logs dbt run failures
- **Timeout handling**: Configured for long-running data processing

## 📊 Monitoring

- **Cloud Run Logs**: View processing logs in Google Cloud Console
- **BigQuery Jobs**: Monitor dbt query performance
- **API Responses**: Track processing metrics and timing

## 🔒 Security

- Uses Google Cloud IAM for authentication
- Stores API keys in Google Secret Manager
- Validates request payloads
- TODO: Implement proper JWT token validation

## 🎯 Next Steps

1. **Implement proper authentication**: Replace placeholder auth with JWT validation
2. **Add job status tracking**: Implement async processing with status polling
3. **Enhanced error recovery**: Add retry logic for transient failures
4. **Performance optimization**: Implement parallel file processing
5. **Cost optimization**: Use Cloud Functions for smaller workloads
