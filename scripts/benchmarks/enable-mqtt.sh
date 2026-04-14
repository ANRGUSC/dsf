#!/usr/bin/env bash
# MQTT is enabled per-template, not cluster-wide. The mqtt-broker Deployment
# and Service live in dsf-system already.
#
# To run a CDAG via MQTT, set these env vars on each task in the template:
#
#   DSF_TRANSPORT_PATTERN=mqtt
#   DSF_MQTT_BROKER=mqtt-broker.dsf-system.svc.cluster.local
#   DSF_MQTT_PORT=1883
#
# See eval/scalability/gen-templates.py for how the scalability benchmarks
# flip between zmq and mqtt programmatically.

set -euo pipefail
cd "$(dirname "$0")/../.."

echo "[mqtt] Ensuring mqtt-broker is running..."
kubectl apply -f eval/scalability/mqtt-broker.yml >/dev/null
kubectl rollout status -n dsf-system deploy/mqtt-broker --timeout=60s

echo "[mqtt] Broker ready at mqtt-broker.dsf-system.svc.cluster.local:1883"
echo
echo "To switch a CDAG template to MQTT, patch each task's env:"
echo '  DSF_TRANSPORT_PATTERN=mqtt'
echo '  DSF_MQTT_BROKER=mqtt-broker.dsf-system.svc.cluster.local'
echo
echo "Example:"
echo '  yq -i ".spec.template.spec.tasks[].env += [{\"name\":\"DSF_TRANSPORT_PATTERN\",\"value\":\"mqtt\"}]" your-cdag-template.yml'
