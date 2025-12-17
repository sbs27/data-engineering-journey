"""
Flask web app wrapper for ETL pipeline.
Cloud Run requires a web server listening on port 8080.
"""

from flask import Flask, jsonify, render_template_string
import subprocess
import os
import sys
import logging
import json
from datetime import datetime

app = Flask(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HTML template for web interface
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Data Engineering ETL Pipeline</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .container { border: 1px solid #ddd; padding: 20px; border-radius: 5px; margin: 20px 0; }
        .button { background: #4285f4; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        .button:hover { background: #3367d6; }
        .success { color: green; }
        .error { color: red; }
        .logs { background: #f5f5f5; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; }
    </style>
</head>
<body>
    <h1> Data Engineering ETL Pipeline</h1>
    
    <div class="container">
        <h2>Cloud Deployment Status:  Running</h2>
        <p><strong>Service:</strong> my-etl-pipeline</p>
        <p><strong>Platform:</strong> Google Cloud Run</p>
        <p><strong>Deployment Time:</strong> {{ deployment_time }}</p>
    </div>
    
    <div class="container">
        <h2>Run ETL Pipeline</h2>
        <p>Click the button below to execute the ETL pipeline:</p>
        <button class="button" onclick="runETL()"> Run ETL Pipeline</button>
        <div id="result"></div>
    </div>
    
    <div class="container">
        <h3>API Endpoints</h3>
        <ul>
            <li><strong>GET /</strong> - This web interface</li>
            <li><strong>GET /health</strong> - Health check</li>
            <li><strong>GET /run-etl</strong> - Run ETL pipeline (returns JSON)</li>
            <li><strong>GET /api/status</strong> - Service status</li>
        </ul>
    </div>
    
    <script>
    function runETL() {
        const resultDiv = document.getElementById('result');
        resultDiv.innerHTML = '<p>⏳ Running ETL pipeline...</p>';
        
        fetch('/run-etl')
            .then(response => response.json())
            .then(data => {
                let html = '<h3 class="success"> ETL Pipeline Complete!</h3>';
                html += '<p><strong>Status:</strong> ' + (data.success ? 'Success' : 'Failed') + '</p>';
                html += '<p><strong>Return Code:</strong> ' + data.return_code + '</p>';
                
                if (data.stdout) {
                    html += '<h4>Output:</h4><div class="logs">' + data.stdout + '</div>';
                }
                
                if (data.stderr) {
                    html += '<h4>Errors:</h4><div class="logs error">' + data.stderr + '</div>';
                }
                
                resultDiv.innerHTML = html;
            })
            .catch(error => {
                resultDiv.innerHTML = '<p class="error"> Error: ' + error + '</p>';
            });
    }
    </script>
</body>
</html>
'''

@app.route('/')
def home():
    """Home page with web interface."""
    return render_template_string(
        HTML_TEMPLATE,
        deployment_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
    )

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "etl-pipeline",
        "timestamp": datetime.now().isoformat(),
        "cloud_platform": "Google Cloud Run"
    })

@app.route('/run-etl')
def run_etl():
    """Run the ETL pipeline and return results."""
    try:
        logger.info("Starting ETL pipeline via HTTP request")
        
        # First, make sure output directory exists
        output_dir = "/app/output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Run the cloud pipeline
        result = subprocess.run(
            [sys.executable, "src/cloud_pipeline.py"],
            capture_output=True,
            text=True,
            cwd="/app",
            timeout=120  # 2 minute timeout
        )
        
        # Check if output was created
        output_files = []
        if os.path.exists(output_dir):
            output_files = os.listdir(output_dir)
        
        # Return results
        return jsonify({
            "success": result.returncode == 0,
            "return_code": result.returncode,
            "stdout": result.stdout[-1000:],  # Last 1000 chars
            "stderr": result.stderr[-1000:],  # Last 1000 chars
            "output_files": output_files,
            "timestamp": datetime.now().isoformat(),
            "message": "ETL pipeline executed successfully" if result.returncode == 0 else "ETL pipeline failed"
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({
            "success": False,
            "error": "ETL pipeline timed out after 2 minutes",
            "timestamp": datetime.now().isoformat()
        }), 500
    except Exception as e:
        logger.error(f"Error running ETL: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/status')
def api_status():
    """API status endpoint."""
    return jsonify({
        "service": "data-engineering-etl",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": [
            {"path": "/", "method": "GET", "description": "Web interface"},
            {"path": "/health", "method": "GET", "description": "Health check"},
            {"path": "/run-etl", "method": "GET", "description": "Execute ETL pipeline"},
            {"path": "/api/status", "method": "GET", "description": "API status"}
        ],
        "timestamp": datetime.now().isoformat()
    })

if __name__ == "__main__":
    # Cloud Run sets PORT environment variable
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
