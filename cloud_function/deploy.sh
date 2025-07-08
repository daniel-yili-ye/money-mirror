#!/bin/bash

# Money Mirror Cloud Run Deployment Script
set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-your-project-id}"
SERVICE_NAME="money-mirror-processor"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "🚀 Deploying Money Mirror Data Processor to Cloud Run"
echo "Project: $PROJECT_ID"
echo "Service: $SERVICE_NAME"
echo "Region: $REGION"

# Build and push Docker image
echo "📦 Building Docker image..."
docker build -t $IMAGE_NAME .

echo "⬆️ Pushing to Container Registry..."
docker push $IMAGE_NAME

# Deploy to Cloud Run
echo "🌐 Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --project $PROJECT_ID \
    --memory 2Gi \
    --cpu 1 \
    --timeout 900 \
    --concurrency 10 \
    --max-instances 3 \
    --set-env-vars "DBT_GCP_PROJECT=${PROJECT_ID}" \
    --allow-unauthenticated

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --project=$PROJECT_ID --format="value(status.url)")

echo "✅ Deployment complete!"
echo "🔗 Service URL: $SERVICE_URL"
echo ""
echo "📝 Next steps:"
echo "1. Add the following to your Streamlit secrets.toml:"
echo ""
echo "[cloud_run]"
echo "process_data_url = \"$SERVICE_URL\""
echo ""
echo "2. Ensure the following secrets are set in Google Secret Manager:"
echo "   - gemini-api-key: Your Gemini API key"
echo ""
echo "3. Initialize categories table (run once):"
echo "   curl -X POST $SERVICE_URL/init-categories" 