"""
TransportRouter: reads DSF_* env vars and instantiates the right transport.

Environment variables read:
    DSF_TRANSPORT_PATTERN   pushpull (default) | pubsub
    DSF_RECV_PORT           PULL bind port for pushpull (default: 5555)
    DSF_PUB_PORT            PUB bind port for pubsub   (default: 5555)
    DSF_PEER_<NAME>         zmq://host:port for peer task <NAME>
                            (NAME is uppercase, hyphens as underscores)
"""

import os
import re

from dsf_sdk.transport.zeromq import ZmqPushPullTransport, ZmqPubSubTransport

_PEER_RE = re.compile(r"^DSF_PEER_([A-Z0-9_]+)$")


def build_transport() -> ZmqPushPullTransport | ZmqPubSubTransport:
    """Build and return the appropriate transport from environment variables."""
    pattern = os.environ.get("DSF_TRANSPORT_PATTERN", "pushpull").lower()

    # Collect peer endpoints from DSF_PEER_* env vars
    peers: dict[str, str] = {}
    for key, value in os.environ.items():
        m = _PEER_RE.match(key)
        if m:
            # DSF_PEER_BRANCH_A -> "branch-a"
            peer_name = m.group(1).lower().replace("_", "-")
            peers[peer_name] = value

    if pattern == "pubsub":
        pub_port = int(os.environ.get("DSF_PUB_PORT", "5555"))
        return ZmqPubSubTransport(pub_port, peers)
    else:
        recv_port = int(os.environ.get("DSF_RECV_PORT", "5555"))
        return ZmqPushPullTransport(recv_port, peers)
