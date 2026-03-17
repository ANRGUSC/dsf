"""
FileTransport: push-based p2p file transport for one-shot ODAGs.

Flow
----
send(payload):
    1. Signal state = Sending to the local data-agent.
    2. Write payload to this task's local hostPath output file.
    3. For each successor task on a *different* node: HTTP PUT the payload
       to the data-agent running on that node.
    4. Signal state = DataReady to the local data-agent.

recv(peer) / recv_all():
    Always a local file read. The odag-controller starts a task pod only
    after all upstream deps are DataReady, so the file is guaranteed present.

State protocol
--------------
Tasks signal their state to the local data-agent (reachable at DSF_NODE_IP:
8081) via PUT /state/<odag>/<task>. The controller queries this endpoint to
decide when to start downstream tasks (DataReady trigger, not PodSucceeded).

States signalled by the SDK:
    Running     — on FileTransport.__init__()
    Sending     — at the start of send()
    DataReady   — after all pushes in send() complete

States derived by the controller from pod events:
    Pending     — no pod yet
    Scheduled   — pod created, not yet Running
    Succeeded   — pod exited 0
    Failed      — pod exited non-0 or send() raised

Environment variables injected by odag-controller
--------------------------------------------------
    DSF_ODAG_NAME               name of this ODAG
    DSF_TASK_NAME               name of this task
    DSF_OUTPUT_DIR              directory where this task should write output
    DSF_DEPS                    comma-separated upstream dependency names
    DSF_NODE_IP                 host IP of this node (for data-agent state PUT)
    NODE_NAME                   this pod's node (downward API)

    DSF_SUCCESSORS              comma-separated successor task names
    DSF_SUCC_<SUCC>_NODE        node name where that successor will run
    DSF_SUCC_<SUCC>_HOST        internal IP of that node (for data-agent PUT)

    (<SUCC> is the task name uppercased with hyphens replaced by underscores)
"""

import os
import threading
import time
import urllib.error
import urllib.request


_DATA_AGENT_PORT = 8081
_PUSH_RETRIES = 5
_PUSH_RETRY_DELAY = 0.5  # seconds


class FileTransport:
    """
    File-based transport for one-shot (ODAG) task graphs.

    send(payload)       — write output and push to all remote successor nodes.
    recv(peer)          — read a specific upstream dep's output (local file).
    recv(peer=None)     — shorthand when there is exactly one dep.
    recv_all()          — read all deps; returns {dep_name: bytes}.
    close()             — no-op (nothing to close).
    """

    def __init__(self) -> None:
        self.odag_name: str = os.environ["DSF_ODAG_NAME"]
        self.task_name: str = os.environ["DSF_TASK_NAME"]
        self.output_dir: str = os.environ["DSF_OUTPUT_DIR"]
        self.node_name: str = os.environ.get("NODE_NAME", "")
        self.node_ip: str = os.environ.get("DSF_NODE_IP", "")
        self._send_thread: threading.Thread | None = None
        self._set_state("Executing")

    # ------------------------------------------------------------------ #
    # state protocol                                                        #
    # ------------------------------------------------------------------ #

    def _set_sending(self, sending: bool) -> None:
        if not self.node_ip:
            return
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/sending/{self.odag_name}/{self.task_name}"
        try:
            body = b"true" if sending else b"false"
            req = urllib.request.Request(url, data=body, method="PUT")
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to set sending={sending}: {e}", flush=True)

    def _set_state(self, state: str) -> None:
        """
        Signal this task's current state to the local data-agent via
        PUT /state/<odag>/<task>.  Failures are logged but never fatal —
        the task's work must not be blocked by a state reporting glitch.
        """
        if not self.node_ip:
            return
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/state/{self.odag_name}/{self.task_name}"
        try:
            req = urllib.request.Request(url, data=state.encode(), method="PUT")
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to set state={state}: {e}", flush=True)

    # ------------------------------------------------------------------ #
    # send                                                                  #
    # ------------------------------------------------------------------ #

    def send(self, payload: bytes) -> None:
        """
        Write payload to this task's local output file and push to every remote
        successor node in a background thread, then return immediately so the
        task can continue running in parallel with the transfer.

        state=Sending is set before the thread starts (visible to controller
        right away). The thread sets state=DataReady when all pushes complete.
        close() joins the thread before setting state=Succeeded.
        """
        self._set_sending(True)

        def _transfer() -> None:
            # 1. Local write — covers same-node successors and our own storage.
            os.makedirs(self.output_dir, exist_ok=True)
            output_path = os.path.join(self.output_dir, "output")
            with open(output_path, "wb") as f:
                f.write(payload)
            print(
                f"[{self.task_name}] wrote {len(payload)} bytes to {output_path}",
                flush=True,
            )

            # 2. Push to each remote successor node.
            succs_env = os.environ.get("DSF_SUCCESSORS", "")
            for succ in [s for s in succs_env.split(",") if s]:
                succ_key = succ.upper().replace("-", "_")
                succ_node = os.environ.get(f"DSF_SUCC_{succ_key}_NODE", "")
                succ_host = os.environ.get(f"DSF_SUCC_{succ_key}_HOST", "")

                if succ_node == self.node_name:
                    # Same node — local file already covers this successor.
                    continue

                if not succ_host:
                    print(
                        f"[{self.task_name}] WARNING: no host for successor {succ}; skipping push",
                        flush=True,
                    )
                    continue

                self._push(succ, succ_host, payload)

            # 3. All pushes complete — data is ready on every successor node.
            self._set_sending(False)
            self._set_state("DataReady")

        self._send_thread = threading.Thread(target=_transfer, daemon=True)
        self._send_thread.start()

    def _push(self, succ: str, host: str, payload: bytes) -> None:
        """HTTP PUT payload to the data-agent on the destination node."""
        url = f"http://{host}:{_DATA_AGENT_PORT}/{self.odag_name}/{self.task_name}/output"
        print(f"[{self.task_name}] pushing to {succ} at {url} ({len(payload)} bytes)", flush=True)

        for attempt in range(1, _PUSH_RETRIES + 1):
            try:
                req = urllib.request.Request(url, data=payload, method="PUT")
                with urllib.request.urlopen(req, timeout=15) as resp:
                    if resp.status == 200:
                        print(f"[{self.task_name}] push to {succ} OK", flush=True)
                        return
            except Exception as e:
                print(
                    f"[{self.task_name}] push to {succ} attempt {attempt}/{_PUSH_RETRIES} failed: {e}",
                    flush=True,
                )
                if attempt < _PUSH_RETRIES:
                    time.sleep(_PUSH_RETRY_DELAY)

        raise RuntimeError(f"[{self.task_name}] failed to push output to successor {succ} after {_PUSH_RETRIES} attempts")

    # ------------------------------------------------------------------ #
    # recv                                                                  #
    # ------------------------------------------------------------------ #

    def recv(self, peer: str | None = None) -> bytes:
        """
        Read the output of an upstream dependency from the local hostPath.

        If peer is None, DSF_DEPS must contain exactly one entry.
        Raises RuntimeError if there are multiple deps and no peer is given.
        """
        if peer is None:
            deps = [d for d in os.environ.get("DSF_DEPS", "").split(",") if d]
            if not deps:
                raise RuntimeError("recv() called with no peer and DSF_DEPS is empty")
            if len(deps) > 1:
                raise RuntimeError(
                    f"recv() called with no peer but task has multiple deps: {deps}. "
                    "Use recv(peer) or recv_all()."
                )
            peer = deps[0]

        path = f"/data/dsf-outputs/{self.odag_name}/{peer}/output"
        with open(path, "rb") as f:
            data = f.read()
        print(f"[{self.task_name}] read {len(data)} bytes from {peer} ({path})", flush=True)
        return data

    def recv_all(self) -> dict[str, bytes]:
        """
        Read outputs from all upstream dependencies.

        Returns a dict mapping dependency name -> bytes.
        """
        deps = [d for d in os.environ.get("DSF_DEPS", "").split(",") if d]
        if not deps:
            raise RuntimeError("recv_all() called but DSF_DEPS is empty")
        return {dep: self.recv(dep) for dep in deps}

    # ------------------------------------------------------------------ #
    # close                                                                 #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        if self._send_thread is not None:
            self._send_thread.join()
        self._set_state("Done")
