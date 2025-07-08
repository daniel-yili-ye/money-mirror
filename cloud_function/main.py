import os
import logging
from flask import Flask, request, jsonify
from google.cloud import secretmanager
import json

from src.data_processor import DataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_secret(secret_name: str, project_id: str) -> str:
    """Retrieve secret from Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@app.route('/process-data', methods=['POST'])
def process_data():
    """
    Main endpoint to trigger data processing pipeline
    
    Expected JSON payload:
    {
        "institution": "amex" | "wealthsimple",
        "file_paths": ["path1", "path2", ...],
        "force_reprocess": false,
        "auth_token": "jwt_token"
    }
    """
    try:
        # Validate request
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        data = request.get_json()
        
        # Basic validation
        required_fields = ["institution", "file_paths"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        # TODO: Validate auth_token here
        
        # Initialize data processor
        project_id = os.environ.get('GCP_PROJECT_ID')
        if not project_id:
            return jsonify({"error": "GCP_PROJECT_ID environment variable not set"}), 500
        
        processor = DataProcessor(
            project_id=project_id,
            dataset_id="personal_finance",
            bucket_name="personal-finance-dashboard"
        )
        
        # Process the data
        result = processor.process_files(
            institution=data["institution"],
            file_paths=data["file_paths"],
            force_reprocess=data.get("force_reprocess", False)
        )
        
        return jsonify({
            "status": "success",
            "message": "Data processing completed",
            "result": result
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Get processing status for a job (future enhancement)"""
    # TODO: Implement job status tracking
    return jsonify({
        "job_id": job_id,
        "status": "completed",
        "progress": 100
    }), 200

@app.route('/init-categories', methods=['POST'])
def init_categories():
    """Initialize the categories dimension table with predefined taxonomy"""
    try:
        # Load categories from the taxonomy
        categories_data = [
            {"general_category": "Groceries", "detailed_category": "Supermarkets"},
            {"general_category": "Groceries", "detailed_category": "Convenience Stores"},
            {"general_category": "Groceries", "detailed_category": "Specialty Food Stores"},
            {"general_category": "Dining & Restaurants", "detailed_category": "Coffee Shops"},
            {"general_category": "Dining & Restaurants", "detailed_category": "Fast Food"},
            {"general_category": "Dining & Restaurants", "detailed_category": "Takeout & Delivery"},
            {"general_category": "Dining & Restaurants", "detailed_category": "Sit-down Restaurants"},
            {"general_category": "Shopping", "detailed_category": "Clothing"},
            {"general_category": "Shopping", "detailed_category": "Electronics"},
            {"general_category": "Shopping", "detailed_category": "Online Retail"},
            {"general_category": "Shopping", "detailed_category": "Department Stores"},
            {"general_category": "Shopping", "detailed_category": "Gifts"},
            {"general_category": "Shopping", "detailed_category": "Beauty & Cosmetics"},
            {"general_category": "Personal Care", "detailed_category": "Salons & Spas"},
            {"general_category": "Personal Care", "detailed_category": "Barbershops"},
            {"general_category": "Personal Care", "detailed_category": "Skincare & Grooming"},
            {"general_category": "Housing & Utilities", "detailed_category": "Rent / Mortgage"},
            {"general_category": "Housing & Utilities", "detailed_category": "Electricity"},
            {"general_category": "Housing & Utilities", "detailed_category": "Water"},
            {"general_category": "Housing & Utilities", "detailed_category": "Gas"},
            {"general_category": "Housing & Utilities", "detailed_category": "Internet"},
            {"general_category": "Housing & Utilities", "detailed_category": "Phone"},
            {"general_category": "Housing & Utilities", "detailed_category": "Trash & Recycling"},
            {"general_category": "Housing & Utilities", "detailed_category": "Home Supplies & Repairs"},
            {"general_category": "Transportation", "detailed_category": "Gas & Fuel"},
            {"general_category": "Transportation", "detailed_category": "Rideshare"},
            {"general_category": "Transportation", "detailed_category": "Public Transit"},
            {"general_category": "Transportation", "detailed_category": "Parking"},
            {"general_category": "Transportation", "detailed_category": "Tolls"},
            {"general_category": "Transportation", "detailed_category": "Vehicle Maintenance"},
            {"general_category": "Transportation", "detailed_category": "Car Insurance"},
            {"general_category": "Financial", "detailed_category": "Credit Card Payments"},
            {"general_category": "Financial", "detailed_category": "Bank Fees"},
            {"general_category": "Financial", "detailed_category": "Investments"},
            {"general_category": "Financial", "detailed_category": "Interest / Dividends"},
            {"general_category": "Financial", "detailed_category": "Loan Payments"},
            {"general_category": "Financial", "detailed_category": "RRSP/TFSA Contributions"},
            {"general_category": "Health & Wellness", "detailed_category": "Pharmacy"},
            {"general_category": "Health & Wellness", "detailed_category": "Medical & Dental"},
            {"general_category": "Health & Wellness", "detailed_category": "Health Insurance"},
            {"general_category": "Health & Wellness", "detailed_category": "Gym / Fitness"},
            {"general_category": "Health & Wellness", "detailed_category": "Sports"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Streaming Services"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Movies & Events"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Travel"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Airbnb"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Subscriptions & Hobbies"},
            {"general_category": "Lifestyle & Entertainment", "detailed_category": "Vacation Spending"},
            {"general_category": "Education", "detailed_category": "Tuition"},
            {"general_category": "Education", "detailed_category": "Online Courses"},
            {"general_category": "Education", "detailed_category": "Books & Materials"},
            {"general_category": "Income & Transfers", "detailed_category": "Salary / Paycheck"},
            {"general_category": "Income & Transfers", "detailed_category": "Cash Back / Rewards"},
            {"general_category": "Income & Transfers", "detailed_category": "Internal Transfers"},
            {"general_category": "Income & Transfers", "detailed_category": "Refunds & Reimbursements"},
            {"general_category": "Work & Business", "detailed_category": "Business Travel"},
            {"general_category": "Work & Business", "detailed_category": "Meals"},
            {"general_category": "Work & Business", "detailed_category": "Contractor Income"},
            {"general_category": "Work & Business", "detailed_category": "Software / Tools"},
            {"general_category": "Uncategorized", "detailed_category": "Uncategorized"}
        ]
        
        project_id = os.environ.get('GCP_PROJECT_ID')
        if not project_id:
            return jsonify({"error": "GCP_PROJECT_ID environment variable not set"}), 500
        
        from src.bigquery_manager import BigQueryManager
        bq_manager = BigQueryManager(project_id, "personal_finance")
        bq_manager.initialize_categories(categories_data)
        
        return jsonify({
            "status": "success",
            "message": f"Initialized categories table with {len(categories_data)} categories"
        }), 200
        
    except Exception as e:
        logger.error(f"Error initializing categories: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False) 