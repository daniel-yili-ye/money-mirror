FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy dbt project
COPY ../dbt_project ./dbt_project/

# Create dbt profiles directory and set up profiles.yml
RUN mkdir -p ~/.dbt && \
    echo "money_mirror:" > ~/.dbt/profiles.yml && \
    echo "  target: prod" >> ~/.dbt/profiles.yml && \
    echo "  outputs:" >> ~/.dbt/profiles.yml && \
    echo "    prod:" >> ~/.dbt/profiles.yml && \
    echo "      type: bigquery" >> ~/.dbt/profiles.yml && \
    echo "      method: service-account" >> ~/.dbt/profiles.yml && \
    echo "      project: \"\{{ env_var('DBT_GCP_PROJECT') }}\"" >> ~/.dbt/profiles.yml && \
    echo "      dataset: personal_finance" >> ~/.dbt/profiles.yml && \
    echo "      location: US" >> ~/.dbt/profiles.yml && \
    echo "      timeout_seconds: 300" >> ~/.dbt/profiles.yml && \
    echo "      priority: interactive" >> ~/.dbt/profiles.yml

# Create empty __init__.py files
RUN touch src/__init__.py

# Expose port
EXPOSE 8080

# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "300", "main:app"] 