"""
ZeroMQ transport implementations for DSF.

Two patterns are supported, selected by DSF_TRANSPORT_PATTERN env var:

  pushpull (default, for one-shot DAGs):
    - Receiver binds a PULL socket at *:DSF_RECV_PORT (default 5555).
    - Sender connects a PUSH socket to the peer's Service endpoint.

  pubsub (for continuous CTGs):
    - Publisher binds a PUB socket at *:DSF_PUB_PORT (default 5555).
    - Subscriber connects a SUB socket to the peer's Service endpoint.
    - Supports targeted publish via topic prefixes and multi-peer polling.

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

    def send(self, payload: bytes) -> None:
        """Connect-PUSH to all configured downstream peers."""
        for peer, endpoint in self._peer_endpoints.items():
            if peer not in self._push:
                sock = self._ctx.socket(zmq.PUSH)
                sock.setsockopt(zmq.LINGER, 5000)
                sock.connect(endpoint.replace("zmq://", "tcp://"))
                self._push[peer] = sock
                time.sleep(0.2)
            self._push[peer].send(payload)
        time.sleep(0.3)

    def recv(self, peer: str | None = None) -> bytes:
        """Bind-PULL: receives from whichever upstream peer pushes first."""
        if self._pull is None:
            self._pull = self._ctx.socket(zmq.PULL)
            self._pull.bind(f"tcp://*:{self._recv_port}")
        return self._pull.recv()

    def recv_all(self) -> dict[str, bytes]:
        raise NotImplementedError("recv_all() is not supported for pushpull transport")

    # Streaming methods not supported for pushpull.
    def publish(self, payload: bytes, topic: bytes = b"") -> None:
        raise NotImplementedError("publish() is not supported for pushpull transport")

    def subscribe(self, peer: str) -> "zmq.Socket":
        raise NotImplementedError("subscribe() is not supported for pushpull transport")

    def poll_subscribers(self, sockets: dict[str, "zmq.Socket"], timeout_ms: int = -1) -> list[tuple[str, bytes]]:
        raise NotImplementedError("poll_subscribers() is not supported for pushpull transport")

    def close(self) -> None:
        for s in self._push.values():
            s.close()
        if self._pull:
            self._pull.close()


class ZmqPubSubTransport:
    """
    PUB/SUB transport for fan-out continuous CDAG communication.

    Publisher binds PUB; subscriber connects SUB to publisher's Service.
    One publisher can serve many subscribers without knowing about them.

    Topic protocol:
      - Broadcast:  topic = b"*"          — all subscribers receive
      - Targeted:   topic = b"<task-name>" — only that task receives
      - Subscribers subscribe to both b"*" and b"<own-task-name>"
    """

    # Special topic for broadcast messages.
    BROADCAST_TOPIC = b"*"

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
        self._task_name: str = ""  # set lazily

    def _get_task_name(self) -> str:
        if not self._task_name:
            import os
            self._task_name = os.environ.get("DSF_TASK_NAME", "unknown")
        return self._task_name

    def _ensure_pub(self) -> zmq.Socket:
        if self._pub is None:
            self._pub = self._ctx.socket(zmq.PUB)
            self._pub.bind(f"tcp://*:{self._pub_port}")
            time.sleep(0.5)  # wait for subscribers to connect
        return self._pub

    # -- Legacy send/recv (backward compat with existing tasks) ---------------

    def send(self, payload: bytes) -> None:
        """Bind-PUB: publish payload to all subscribers (broadcast)."""
        pub = self._ensure_pub()
        task_name = self._get_task_name()
        pub.send_multipart([task_name.encode(), payload])

    def recv(self, peer: str | None = None) -> bytes:
        """Connect-SUB to the given peer's PUB socket and receive one message."""
        if peer is None:
            raise ValueError("pubsub transport requires a peer name for recv()")
        sock = self._ensure_sub(peer)
        frames = sock.recv_multipart()
        return frames[1] if len(frames) == 2 else frames[0]

    def recv_all(self) -> dict[str, bytes]:
        raise NotImplementedError("recv_all() is not supported for pubsub transport")

    # -- New streaming API ----------------------------------------------------

    def publish(self, payload: bytes, topic: bytes = b"") -> None:
        """
        Publish a message with a topic prefix.

        Args:
            payload: The message bytes.
            topic:   b"*" for broadcast, or b"<task-name>" for targeted delivery.
                     Empty bytes defaults to broadcast.
        """
        pub = self._ensure_pub()
        if not topic:
            topic = self.BROADCAST_TOPIC
        pub.send_multipart([topic, payload])

    def _ensure_sub(self, peer: str) -> zmq.Socket:
        """Get or create a SUB socket for a peer, subscribed to broadcast + own name."""
        if peer not in self._sub:
            endpoint = self._peer_endpoints.get(peer)
            if not endpoint:
                raise ValueError(
                    f"No endpoint for peer '{peer}'. "
                    f"Set DSF_PEER_{peer.upper().replace('-', '_')} env var."
                )
            sock = self._ctx.socket(zmq.SUB)
            # Subscribe to broadcast messages and messages targeted at this task.
            sock.setsockopt(zmq.SUBSCRIBE, self.BROADCAST_TOPIC)
            sock.setsockopt(zmq.SUBSCRIBE, self._get_task_name().encode())
            sock.connect(endpoint.replace("zmq://", "tcp://"))
            self._sub[peer] = sock
        return self._sub[peer]

    def subscribe(self, peer: str) -> zmq.Socket:
        """
        Create/get a SUB socket for a peer, ready for iteration.

        Returns the raw zmq.Socket so the caller can recv in a loop.
        """
        return self._ensure_sub(peer)

    def poll_subscribers(self, sockets: dict[str, zmq.Socket],
                         timeout_ms: int = -1) -> list[tuple[str, bytes]]:
        """
        Poll multiple SUB sockets and return all ready messages.

        Args:
            sockets:    {peer_name: zmq.Socket} to poll.
            timeout_ms: Polling timeout. -1 = block until at least one message.

        Returns:
            List of (peer_name, payload_bytes) for each ready socket.
        """
        poller = zmq.Poller()
        for name, sock in sockets.items():
            poller.register(sock, zmq.POLLIN)

        ready = dict(poller.poll(timeout_ms))

        results: list[tuple[str, bytes]] = []
        for name, sock in sockets.items():
            if sock in ready:
                frames = sock.recv_multipart(zmq.NOBLOCK)
                payload = frames[1] if len(frames) == 2 else frames[0]
                results.append((name, payload))
        return results

    def close(self) -> None:
        if self._pub:
            self._pub.close()
        for s in self._sub.values():
            s.close()
