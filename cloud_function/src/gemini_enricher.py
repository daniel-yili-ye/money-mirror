import logging
import json
from typing import List, Dict, Any
import google.generativeai as genai
from google.cloud import secretmanager

logger = logging.getLogger(__name__)

class GeminiEnricher:
    """Handle transaction categorization using Gemini API"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.model_name = "gemini-2.5-flash"
        
        # Get API key from Secret Manager
        api_key = self._get_secret("gemini-api-key")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.model_name)
        
        # Define category taxonomy
        self.category_taxonomy = self._get_category_taxonomy()
    
    def _get_secret(self, secret_name: str) -> str:
        """Get secret from Google Secret Manager"""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    def _get_category_taxonomy(self) -> Dict[str, List[str]]:
        """Define the category taxonomy from the categories.csv data"""
        return {
            "Groceries": ["Supermarkets", "Convenience Stores", "Specialty Food Stores"],
            "Dining & Restaurants": ["Coffee Shops", "Fast Food", "Takeout & Delivery", "Sit-down Restaurants"],
            "Shopping": ["Clothing", "Electronics", "Online Retail", "Department Stores", "Gifts", "Beauty & Cosmetics"],
            "Personal Care": ["Salons & Spas", "Barbershops", "Skincare & Grooming"],
            "Housing & Utilities": ["Rent / Mortgage", "Electricity", "Water", "Gas", "Internet", "Phone", "Trash & Recycling", "Home Supplies & Repairs"],
            "Transportation": ["Gas & Fuel", "Rideshare", "Public Transit", "Parking", "Tolls", "Vehicle Maintenance", "Car Insurance"],
            "Financial": ["Credit Card Payments", "Bank Fees", "Investments", "Interest / Dividends", "Loan Payments", "RRSP/TFSA Contributions"],
            "Health & Wellness": ["Pharmacy", "Medical & Dental", "Health Insurance", "Gym / Fitness", "Sports"],
            "Lifestyle & Entertainment": ["Streaming Services", "Movies & Events", "Travel", "Airbnb", "Subscriptions & Hobbies", "Vacation Spending"],
            "Education": ["Tuition", "Online Courses", "Books & Materials"],
            "Income & Transfers": ["Salary / Paycheck", "Cash Back / Rewards", "Internal Transfers", "Refunds & Reimbursements"],
            "Work & Business": ["Business Travel", "Meals", "Contractor Income", "Software / Tools"],
            "Uncategorized": ["Uncategorized"]
        }
    
    def categorize_descriptions(self, descriptions: List[str]) -> List[Dict[str, Any]]:
        """Categorize a batch of transaction descriptions using Gemini"""
        logger.info(f"Categorizing {len(descriptions)} descriptions with Gemini")
        
        if not descriptions:
            return []
        
        # Process in batches to avoid token limits
        batch_size = 20
        all_results = []
        
        for i in range(0, len(descriptions), batch_size):
            batch = descriptions[i:i + batch_size]
            batch_results = self._categorize_batch(batch)
            all_results.extend(batch_results)
        
        logger.info(f"Successfully categorized {len(all_results)} descriptions")
        return all_results
    
    def _categorize_batch(self, descriptions: List[str]) -> List[Dict[str, Any]]:
        """Categorize a single batch of descriptions"""
        try:
            # Create prompt with taxonomy and descriptions
            prompt = self._create_categorization_prompt(descriptions)
            
            # Call Gemini
            response = self.model.generate_content(prompt)
            
            # Parse response
            results = self._parse_gemini_response(response.text, descriptions)
            
            return results
            
        except Exception as e:
            logger.error(f"Error categorizing batch: {str(e)}", exc_info=True)
            # Return fallback categories
            return [{
                'description_key': desc.upper().strip(),
                'original_description': desc,
                'general_category': 'Uncategorized',
                'detailed_category': 'Uncategorized',
                'confidence_score': 0.0,
                'gemini_model_version': self.model_name
            } for desc in descriptions]
    
    def _create_categorization_prompt(self, descriptions: List[str]) -> str:
        """Create a structured prompt for Gemini categorization"""
        
        # Format taxonomy for prompt
        taxonomy_text = ""
        for general_cat, detailed_cats in self.category_taxonomy.items():
            taxonomy_text += f"**{general_cat}:**\n"
            for detailed_cat in detailed_cats:
                taxonomy_text += f"  - {detailed_cat}\n"
            taxonomy_text += "\n"
        
        # Format descriptions list
        descriptions_text = ""
        for i, desc in enumerate(descriptions, 1):
            descriptions_text += f"{i}. {desc}\n"
        
        prompt = f"""
You are a financial transaction categorizer. Categorize each transaction description into the most appropriate category from the provided taxonomy.

**CATEGORY TAXONOMY:**
{taxonomy_text}

**INSTRUCTIONS:**
1. For each transaction description, choose the MOST SPECIFIC detailed category that fits
2. If no specific category fits well, use the appropriate general category with "Uncategorized" as detailed
3. Provide a confidence score (0.0-1.0) for each categorization
4. Return your response as a JSON array with this exact format:

```json
[
  {{
    "description_number": 1,
    "general_category": "Category Name",
    "detailed_category": "Subcategory Name", 
    "confidence_score": 0.85
  }},
  ...
]
```

**TRANSACTION DESCRIPTIONS TO CATEGORIZE:**
{descriptions_text}

Respond ONLY with the JSON array, no additional text.
"""
        
        return prompt
    
    def _parse_gemini_response(self, response_text: str, original_descriptions: List[str]) -> List[Dict[str, Any]]:
        """Parse Gemini response and create standardized results"""
        try:
            # Extract JSON from response
            response_text = response_text.strip()
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            
            # Parse JSON
            categorizations = json.loads(response_text.strip())
            
            # Create standardized results
            results = []
            for i, cat in enumerate(categorizations):
                if i < len(original_descriptions):
                    desc = original_descriptions[i]
                    result = {
                        'description_key': desc.upper().strip(),
                        'original_description': desc,
                        'general_category': cat.get('general_category', 'Uncategorized'),
                        'detailed_category': cat.get('detailed_category', 'Uncategorized'),
                        'confidence_score': float(cat.get('confidence_score', 0.0)),
                        'gemini_model_version': self.model_name
                    }
                    results.append(result)
            
            return results
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error parsing Gemini response: {str(e)}, Response: {response_text}")
            # Return fallback categories
            return [{
                'description_key': desc.upper().strip(),
                'original_description': desc,
                'general_category': 'Uncategorized',
                'detailed_category': 'Uncategorized',
                'confidence_score': 0.0,
                'gemini_model_version': self.model_name
            } for desc in original_descriptions] 