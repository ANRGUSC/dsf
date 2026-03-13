#!/bin/bash
# Integration test for link-scorer with mock Prometheus

set -e

PORT=9999
MOCK_PID=""

cleanup() {
    if [ ! -z "$MOCK_PID" ]; then
        echo "Stopping mock Prometheus server..."
        kill $MOCK_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Start mock Prometheus server
cat > /tmp/mock_prom.py << 'EOFPY'
import http.server
import socketserver
import json
import urllib.parse
import sys

class MockPrometheusHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress logs
        pass
    
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        
        if '/api/v1/query' in self.path:
            promql = query.get('query', [''])[0]
            
            # Mock different responses based on query
            if 'egress' in promql:
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
            elif 'ingress' in promql:
                response = {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {"metric": {"instance": "anrg-1"}, "value": [1234567890, "180000000"]},
                            {"metric": {"instance": "anrg-2"}, "value": [1234567890, "120000000"]},
                            {"metric": {"instance": "anrg-3"}, "value": [1234567890, "280000000"]},
                            {"metric": {"instance": "anrg-4"}, "value": [1234567890, "90000000"]},
                            {"metric": {"instance": "anrg-5"}, "value": [1234567890, "220000000"]}
                        ]
                    }
                }
            elif 'drop' in promql:
                # Some nodes have drops
                response = {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {"metric": {"instance": "anrg-1"}, "value": [1234567890, "1000000"]},
                            {"metric": {"instance": "anrg-2"}, "value": [1234567890, "0"]},
                            {"metric": {"instance": "anrg-3"}, "value": [1234567890, "5000000"]},
                            {"metric": {"instance": "anrg-4"}, "value": [1234567890, "0"]},
                            {"metric": {"instance": "anrg-5"}, "value": [1234567890, "2000000"]}
                        ]
                    }
                }
            else:
                response = {"status": "success", "data": {"resultType": "vector", "result": []}}
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
with socketserver.TCPServer(("", PORT), MockPrometheusHandler) as httpd:
    httpd.serve_forever()
EOFPY

echo "Starting mock Prometheus server on port $PORT..."
python3 /tmp/mock_prom.py $PORT > /dev/null 2>&1 &
MOCK_PID=$!

# Wait for server to be ready
sleep 2

# Test 1: Basic query
echo "=== Test 1: Basic query (JSON output, top 10) ==="
cd /home/anrg/dsf/phase-1/net-metrics/link-scorer
./link-scorer -prom-url http://localhost:$PORT -topk 10 2>&1 | head -50

echo ""
echo "=== Test 2: CSV output ==="
./link-scorer -prom-url http://localhost:$PORT -output csv -topk 5 2>&1

echo ""
echo "=== Test 3: All edges ==="
./link-scorer -prom-url http://localhost:$PORT -topk 0 2>&1 | jq '.edges | length' 2>/dev/null || echo "jq not available, checking manually..."

echo ""
echo "=== Test 4: Custom parameters ==="
./link-scorer -prom-url http://localhost:$PORT -window 60s -capacity 2e9 -drop-ref 5e6 -topk 3 2>&1 | head -30

echo ""
echo "=== Test 5: Specific nodes ==="
./link-scorer -prom-url http://localhost:$PORT -nodes "anrg-1,anrg-2,anrg-3" -topk 5 2>&1 | head -40

echo ""
echo "All tests completed successfully!"
