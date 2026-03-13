"""
DSFTask: the main user-facing class for task communication.

Usage in task images:

    from dsf_sdk import DSFTask

    task = DSFTask()

    # One-shot DAG (pushpull):
    data = task.recv("upstream-task")   # blocks until data arrives
    result = process(data)
    task.send("downstream-task", result)

    # Continuous CTG (pubsub):
    while True:
        item = task.recv("upstream-task")   # blocks until next message
        result = process(item)
        task.send("downstream-task", result)

The controller injects these environment variables:
    DSF_TASK_NAME             name of this task
    DSF_TRANSPORT_PATTERN     pushpull | pubsub
    DSF_RECV_PORT             PULL bind port (pushpull mode)
    DSF_PUB_PORT              PUB bind port  (pubsub mode)
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
    environment variables injected by the dag-controller or ctg-controller.
    """

    def __init__(self) -> None:
        self.name: str = os.environ.get("DSF_TASK_NAME", "unknown")
        self._transport = build_transport()
        pattern = os.environ.get("DSF_TRANSPORT_PATTERN", "pushpull")
        print(f"[{self.name}] DSFTask initialized (transport: {pattern})", flush=True)

    def send(self, peer: str, data: Any) -> None:
        """
        Send data to a peer task.

        In pushpull mode: PUSH to peer's PULL socket.
        In pubsub mode:   PUB to all subscribers (peer name used as topic).

        Args:
            peer: name of the destination task (e.g. "transform", "sink").
            data: JSON-serializable value.
        """
        payload = json.dumps(data).encode()
        self._transport.send(peer, payload)

    def recv(self, peer: str | None = None) -> Any:
        """
        Receive data from an upstream task. Blocks until data arrives.

        In pushpull mode: PULL (binds locally; peer arg is cosmetic).
        In pubsub mode:   SUB to peer's PUB socket (peer arg required).

        Args:
            peer: name of the upstream task (required in pubsub mode).

        Returns:
            The deserialized value sent by the upstream task.
        """
        payload = self._transport.recv(peer)
        return json.loads(payload)

    def close(self) -> None:
        """Close all open sockets. Call on shutdown."""
        self._transport.close()
