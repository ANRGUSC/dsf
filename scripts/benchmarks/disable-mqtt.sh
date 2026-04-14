#!/usr/bin/env bash
# MQTT is per-template. This script just reminds you how to revert a CDAG
# template's task env vars from MQTT back to ZeroMQ pubsub.

set -euo pipefail

cat <<'EOF'
To revert a CDAG template to ZeroMQ pubsub, remove these from each task's env:

  DSF_TRANSPORT_PATTERN=mqtt
  DSF_MQTT_BROKER=...
  DSF_MQTT_PORT=...

The default (no DSF_TRANSPORT_PATTERN) is pubsub for CDAG controllers.

To also tear down the broker (frees cluster RAM, idle cost is small):

  kubectl delete deploy,svc -n dsf-system mqtt-broker
EOF
