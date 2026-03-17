"""
DSFTask: the main user-facing class for task communication.

Usage in task images:

    from dsf_sdk import DSFTask

    task = DSFTask()

    # One-shot ODAG (file transport — layer-by-layer execution):
    data  = task.recv("upstream-task")   # read one upstream's output
    inputs = task.recv_all()             # read all upstreams at once -> dict
    task.send(result)                    # routes to all successors automatically

    # Continuous CDAG (pubsub transport):
    while True:
        item = task.recv("upstream-task")
        result = process(item)
        task.send(result)

The odag-controller injects (file transport):
    DSF_TRANSPORT_PATTERN     file
    DSF_ODAG_NAME             ODAG CR name
    DSF_TASK_NAME             this task's name
    DSF_OUTPUT_DIR            where this task writes its output
    DSF_DEPS                  comma-separated dependency names
    DSF_SUCCESSORS            comma-separated successor names
    DSF_NODE_IP               host IP for data-agent state reporting
    NODE_NAME                 this pod's node (downward API)

The cdag-controller injects (pubsub transport):
    DSF_TRANSPORT_PATTERN     pubsub
    DSF_TASK_NAME             this task's name
    DSF_PUB_PORT              PUB bind port
    DSF_PEER_<NAME>           zmq://host:port for each peer task
"""

import json
import os
import sys
from typing import Any

from dsf_sdk.transport.router import build_transport

# Ensure stdout/stderr are line-buffered so logs are visible in kubectl logs.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)  # type: ignore[union-attr]


class DSFTask:
    """
    Entry point for DSF task communication.

    Instantiate once at the start of your task. Reads configuration from
    environment variables injected by the odag-controller or cdag-controller.
    """

    def __init__(self) -> None:
        self.name: str = os.environ.get("DSF_TASK_NAME", "unknown")
        self._transport = build_transport()
        pattern = os.environ.get("DSF_TRANSPORT_PATTERN", "pushpull")
        print(f"[{self.name}] DSFTask initialized (transport: {pattern})", flush=True)

    def send(self, data: Any) -> None:
        """
        Send data to all downstream successors.

        For file transport (ODAG): routes to each successor automatically
        based on DSF_SUCCESSORS env vars injected by the controller.
        Same-node successors receive a local file copy; remote successors
        receive an HTTP PUT via the data-agent.

        For pubsub transport (CDAG): publishes to all subscribers.

        Args:
            data: JSON-serializable value.
        """
        payload = json.dumps(data).encode()
        self._transport.send(payload)

    def recv(self, peer: str | None = None) -> Any:
        """
        Receive data from an upstream task.

        For file transport: reads from the local hostPath output file written
        by the upstream task. Peer defaults to the single dep in DSF_DEPS.

        For pubsub transport: subscribes to peer's PUB socket (peer required).

        Args:
            peer: name of the upstream task. Required when there are multiple
                  upstream dependencies; omit if there is exactly one.

        Returns:
            The deserialized value sent by the upstream task.
        """
        payload = self._transport.recv(peer)
        return json.loads(payload)

    def recv_all(self) -> dict[str, Any]:
        """
        Receive data from all upstream dependencies at once.

        Returns:
            A dict mapping dependency name -> deserialized value.
            Keys match the names in DSF_DEPS / spec.tasks[].dependencies.
        """
        raw = self._transport.recv_all()
        return {k: json.loads(v) for k, v in raw.items()}

    def close(self) -> None:
        """Close all open sockets / file handles. Call on shutdown."""
        self._transport.close()
