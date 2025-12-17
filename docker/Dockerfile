FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/output /app/data /app/logs

ENV PYTHONPATH=/app/src:$PYTHONPATH

# Install Flask for the web server (if not already in requirements.txt)
RUN pip install --no-cache-dir flask

# Create the web server wrapper script
RUN echo 'import os, sys, subprocess, json, time
from flask import Flask, jsonify
app = Flask(__name__)
port = int(os.getenv("PORT", 8080))
@app.route("/")
def home():
    return jsonify({"service": "ETL Pipeline", "status": "running", "port": port})
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})
@app.route("/run")
def run_etl():
    try:
        result = subprocess.run([sys.executable, "src/etl_pipeline.py"], capture_output=True, text=True, timeout=300)
        return jsonify({"success": result.returncode == 0, "stdout": result.stdout[-500:], "stderr": result.stderr[-500:]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port, debug=False)' > /app/server.py

# Check if PORT environment variable is set (Cloud Run sets this)
# If yes, run web server. If no, run batch ETL directly.
CMD ["sh", "-c", "if [ -n \"\$PORT\" ]; then python /app/server.py; else python src/etl_pipeline.py; fi"]
