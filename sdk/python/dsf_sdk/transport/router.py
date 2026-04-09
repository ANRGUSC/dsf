"""
TransportRouter: reads DSF_TRANSPORT_PATTERN and instantiates the right transport.

    DSF_TRANSPORT_PATTERN   file (ODAG) | pubsub (CDAG) | pushpull (legacy)
"""

import os
import re


def build_transport():
    """Build and return the appropriate transport from environment variables."""
    pattern = os.environ.get("DSF_TRANSPORT_PATTERN", "pushpull").lower()

    if pattern == "file":
        from dsf_sdk.transport.file import FileTransport
        return FileTransport()

    if pattern == "mqtt":
        from dsf_sdk.transport.mqtt import MqttTransport
        peers: dict[str, str] = {}
        return MqttTransport(peers)

    if pattern == "shared_volume":
        from dsf_sdk.transport.shared_volume import SharedVolumeTransport
        return SharedVolumeTransport()

    # ZMQ transports (CDAG pubsub, or legacy pushpull)
    from dsf_sdk.transport.zeromq import ZmqPushPullTransport, ZmqPubSubTransport

    _PEER_RE = re.compile(r"^DSF_PEER_([A-Z0-9_]+)$")
    peers: dict[str, str] = {}
    for key, value in os.environ.items():
        m = _PEER_RE.match(key)
        if m:
            peer_name = m.group(1).lower().replace("_", "-")
            peers[peer_name] = value

    if pattern == "pubsub":
        pub_port = int(os.environ.get("DSF_PUB_PORT", "5555"))
        return ZmqPubSubTransport(pub_port, peers)
    else:
        recv_port = int(os.environ.get("DSF_RECV_PORT", "5555"))
        return ZmqPushPullTransport(recv_port, peers)
