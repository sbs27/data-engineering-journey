#!/usr/bin/env python3
import http.server
import socketserver
import subprocess
import json
import os
import sys

PORT = int(os.environ.get("PORT", 8080))

class ETLSHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            response_html = """
                <html><body>
                <h1>ETL Pipeline Server</h1>
                <p>This service wraps the ETL batch job for Google Cloud Run.</p>
                <p><a href="/run-etl">Click here to run the ETL job now</a></p>
                <p><a href="/health">Service health check</a></p>
                </body></html>
            """
            self.wfile.write(response_html.encode())
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            health_status = {"status": "healthy", "service": "etl-server-wrapper"}
            self.wfile.write(json.dumps(health_status).encode())
        elif self.path == '/run-etl':
            try:
                result = subprocess.run(
                    [sys.executable, "src/etl_pipeline.py"],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd="/app"
                )
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                result_payload = {
                    "success": result.returncode == 0,
                    "return_code": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "message": "ETL job execution completed."
                }
                self.wfile.write(json.dumps(result_payload, indent=2).encode())
            except subprocess.TimeoutExpired:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "ETL job timed out after 5 minutes."}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'404 - Not Found')

    def log_message(self, format, *args):
        pass

print(f"Starting wrapper server on port {PORT}")
with socketserver.TCPServer(("", PORT), ETLSHandler) as httpd:
    httpd.serve_forever()
