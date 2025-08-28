# Gunakan image Python
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy semua source code
COPY . .

# Command default (bisa diganti untuk Airflow/ETL)
CMD ["python", "src/main.py"]
