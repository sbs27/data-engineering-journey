import http.server
import socketserver
import subprocess
import threading
import time
import os
import json
from datetime import datetime

PORT = int(os.environ.get("PORT", 8080))

class SchedulerHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "service": "etl-scheduler",
                "scheduler": "running",
                "timestamp": datetime.now().isoformat(),
                "environment": "production"
            }
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html = """<html>
            <head><title>ETL Pipeline Scheduler</title></head>
            <body>
            <h1>ETL Pipeline Scheduler</h1>
            <p>Service is running in production mode.</p>
            <p><strong>Endpoints:</strong></p>
            <ul>
            <li><a href='/health'>/health</a> - Service health check</li>
            <li><a href='/run-now'>/run-now</a> - Run ETL immediately</li>
            <li><a href='/schedule'>/schedule</a> - View schedule</li>
            <li><a href='/status'>/status</a> - System status</li>
            </ul>
            </body></html>"""
            self.wfile.write(html.encode())
            
        elif self.path == '/run-now':
            def run_etl_background():
                try:
                    print(f"{datetime.now().isoformat()} - Manual ETL triggered via API")
                    result = subprocess.run(
                        ['python', 'src/scheduled_etl.py'], 
                        capture_output=True, 
                        text=True,
                        cwd='/app'
                    )
                    if result.returncode == 0:
                        print(f"{datetime.now().isoformat()} - Manual ETL completed successfully")
                    else:
                        print(f"{datetime.now().isoformat()} - Manual ETL failed: {result.stderr}")
                except Exception as e:
                    print(f"{datetime.now().isoformat()} - Manual ETL error: {str(e)}")
            
            threading.Thread(target=run_etl_background, daemon=True).start()
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "message": "ETL pipeline started in background",
                "status": "processing",
                "timestamp": datetime.now().isoformat(),
                "note": "Check logs for execution details"
            }
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/schedule':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "schedule": [
                    {"time": "Daily at 02:00 AM", "description": "Main production ETL run"},
                    {"time": "Every 5 minutes", "description": "Test/demo schedule"},
                    {"time": "On-demand", "description": "Via /run-now endpoint"}
                ],
                "environment": "production",
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "operational",
                "service": "etl-scheduler",
                "uptime": "running",
                "schedule_active": True,
                "timestamp": datetime.now().isoformat(),
                "endpoints_available": ["/health", "/run-now", "/schedule", "/status", "/"]
            }
            self.wfile.write(json.dumps(response).encode())
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "error": "Endpoint not found",
                "available_endpoints": ["/", "/health", "/run-now", "/schedule", "/status"]
            }
            self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        print(f"{datetime.now().isoformat()} - HTTP - {format % args}")

def start_cron_scheduler():
    print(f"{datetime.now().isoformat()} - Starting production scheduler")
    print(f"{datetime.now().isoformat()} - Schedule: Runs every 5 minutes for demo")
    
    last_run = None
    
    def scheduled_task():
        nonlocal last_run
        while True:
            now = datetime.now()
            
            if now.minute % 5 == 0 and now.second < 5:
                if last_run is None or (now - last_run).seconds > 240:
                    print(f"{datetime.now().isoformat()} - Scheduled ETL execution starting")
                    try:
                        result = subprocess.run(
                            ['python', 'src/scheduled_etl.py'], 
                            capture_output=True, 
                            text=True,
                            cwd='/app'
                        )
                        if result.returncode == 0:
                            print(f"{datetime.now().isoformat()} - Scheduled ETL completed successfully")
                        else:
                            print(f"{datetime.now().isoformat()} - Scheduled ETL failed: {result.stderr}")
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} - Scheduled ETL error: {str(e)}")
                    last_run = datetime.now()
            
            time.sleep(1)
    
    scheduler_thread = threading.Thread(target=scheduled_task, daemon=True)
    scheduler_thread.start()
    return scheduler_thread

if __name__ == "__main__":
    print(f"{datetime.now().isoformat()} - ========================================")
    print(f"{datetime.now().isoformat()} - ETL SCHEDULER SERVICE STARTING")
    print(f"{datetime.now().isoformat()} - Port: {PORT}")
    print(f"{datetime.now().isoformat()} - Environment: Production")
    print(f"{datetime.now().isoformat()} - ========================================")
    
    start_cron_scheduler()
    
    with socketserver.TCPServer(("", PORT), SchedulerHandler) as httpd:
        print(f"{datetime.now().isoformat()} - HTTP server started on port {PORT}")
        print(f"{datetime.now().isoformat()} - Service ready for requests")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print(f"{datetime.now().isoformat()} - Server shutting down")
