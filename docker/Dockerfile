FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Create output directory
RUN mkdir -p /app/output

# Run as root (simpler for learning)
CMD ["python", "etl_pipeline.py"]
