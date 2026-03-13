"""
ZeroMQ transport implementations for DSF.

Two patterns are supported, selected by DSF_TRANSPORT_PATTERN env var:

  pushpull (default, for one-shot DAGs):
    - Receiver binds a PULL socket at *:DSF_RECV_PORT (default 5555).
    - Sender connects a PUSH socket to the peer's Service endpoint.

  pubsub (for continuous CTGs):
    - Publisher binds a PUB socket at *:DSF_PUB_PORT (default 5555).
    - Subscriber connects a SUB socket to the peer's Service endpoint.

Both patterns are extensible: add new transport classes here and register
them in transport/router.py.
"""

import time
import zmq


class ZmqPushPullTransport:
    """
    PUSH/PULL transport for point-to-point one-shot DAG communication.

    Receiver binds PULL; sender connects PUSH to receiver's Service.
    Multiple senders can connect to one receiver (fair-queued by ZMQ).
    """

    def __init__(self, recv_port: int, peer_endpoints: dict[str, str]) -> None:
        """
        Args:
            recv_port:      Port this task's PULL socket will bind on.
            peer_endpoints: {peer_name: "zmq://host:port"} for outgoing connections.
        """
        self._ctx = zmq.Context.instance()
        self._recv_port = recv_port
        self._peer_endpoints = peer_endpoints  # name -> zmq://host:port
        self._pull: zmq.Socket | None = None
        self._push: dict[str, zmq.Socket] = {}

    def send(self, peer: str, payload: bytes) -> None:
        """Connect-PUSH to peer's PULL socket (identified by DSF_PEER_<PEER> endpoint)."""
        if peer not in self._push:
            endpoint = self._peer_endpoints.get(peer)
            if not endpoint:
                raise ValueError(
                    f"No endpoint for peer '{peer}'. "
                    f"Set DSF_PEER_{peer.upper().replace('-', '_')} env var."
                )
            sock = self._ctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.LINGER, 5000)  # wait up to 5s for delivery on close
            sock.connect(endpoint.replace("zmq://", "tcp://"))
            self._push[peer] = sock
            time.sleep(0.2)  # allow connection to establish before first send
        self._push[peer].send(payload)
        time.sleep(0.3)  # let the I/O thread flush to TCP before caller exits

    def recv(self, peer: str | None = None) -> bytes:
        """Bind-PULL: receives from whichever upstream peer pushes first."""
        if self._pull is None:
            self._pull = self._ctx.socket(zmq.PULL)
            self._pull.bind(f"tcp://*:{self._recv_port}")
        return self._pull.recv()

    def close(self) -> None:
        for s in self._push.values():
            s.close()
        if self._pull:
            self._pull.close()


class ZmqPubSubTransport:
    """
    PUB/SUB transport for fan-out continuous CTG communication.

    Publisher binds PUB; subscriber connects SUB to publisher's Service.
    One publisher can serve many subscribers without knowing about them.
    """

    def __init__(self, pub_port: int, peer_endpoints: dict[str, str]) -> None:
        """
        Args:
            pub_port:       Port this task's PUB socket will bind on.
            peer_endpoints: {peer_name: "zmq://host:port"} for SUB connections.
        """
        self._ctx = zmq.Context.instance()
        self._pub_port = pub_port
        self._peer_endpoints = peer_endpoints
        self._pub: zmq.Socket | None = None
        self._sub: dict[str, zmq.Socket] = {}

    def send(self, peer: str, payload: bytes) -> None:
        """Bind-PUB: publish to all subscribers (peer arg is used as topic prefix)."""
        if self._pub is None:
            self._pub = self._ctx.socket(zmq.PUB)
            self._pub.bind(f"tcp://*:{self._pub_port}")
            time.sleep(0.5)  # wait for subscribers to connect
        # Send as two-frame: topic + payload (topic = peer name for filtering)
        self._pub.send_multipart([peer.encode(), payload])

    def recv(self, peer: str | None = None) -> bytes:
        """Connect-SUB to the given peer's PUB socket and receive one message."""
        if peer is None:
            raise ValueError("pubsub transport requires a peer name for recv()")
        if peer not in self._sub:
            endpoint = self._peer_endpoints.get(peer)
            if not endpoint:
                raise ValueError(
                    f"No endpoint for peer '{peer}'. "
                    f"Set DSF_PEER_{peer.upper().replace('-', '_')} env var."
                )
            sock = self._ctx.socket(zmq.SUB)
            sock.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all topics
            sock.connect(endpoint.replace("zmq://", "tcp://"))
            self._sub[peer] = sock
        frames = self._sub[peer].recv_multipart()
        # Return the payload frame (second frame)
        return frames[1] if len(frames) == 2 else frames[0]

    def close(self) -> None:
        if self._pub:
            self._pub.close()
        for s in self._sub.values():
            s.close()
