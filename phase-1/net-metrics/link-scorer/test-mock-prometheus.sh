#!/bin/bash
# Mock Prometheus server for testing link-scorer

PORT=9999

# Start a simple HTTP server that responds like Prometheus
python3 -m http.server $PORT --bind 127.0.0.1 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Create mock response
cat > /tmp/mock_prom_response.json << 'EOF'
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"instance": "anrg-1"},
        "value": [1234567890, "200000000"]
      },
      {
        "metric": {"instance": "anrg-2"},
        "value": [1234567890, "150000000"]
      },
      {
        "metric": {"instance": "anrg-3"},
        "value": [1234567890, "300000000"]
      }
    ]
  }
}
EOF

# Create a simple Python server that serves the mock response
cat > /tmp/mock_prom.py << 'EOFPY'
import http.server
import socketserver
import json
import sys

class MockPrometheusHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if '/api/v1/query' in self.path:
            # Mock response
            response = {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {"metric": {"instance": "anrg-1"}, "value": [1234567890, "200000000"]},
                        {"metric": {"instance": "anrg-2"}, "value": [1234567890, "150000000"]},
                        {"metric": {"instance": "anrg-3"}, "value": [1234567890, "300000000"]},
                        {"metric": {"instance": "anrg-4"}, "value": [1234567890, "100000000"]},
                        {"metric": {"instance": "anrg-5"}, "value": [1234567890, "250000000"]}
                    ]
                }
            }
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
with socketserver.TCPServer(("", PORT), MockPrometheusHandler) as httpd:
    print(f"Mock Prometheus server on port {PORT}")
    httpd.serve_forever()
EOFPY

# Kill the simple server
kill $SERVER_PID 2>/dev/null

# Start mock Prometheus
python3 /tmp/mock_prom.py $PORT &
MOCK_PID=$!

echo "Mock Prometheus server started on port $PORT (PID: $MOCK_PID)"
echo "Press Ctrl+C to stop"

# Wait for user interrupt
trap "kill $MOCK_PID; exit" INT TERM
wait $MOCK_PID
