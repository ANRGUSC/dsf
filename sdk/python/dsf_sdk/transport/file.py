"""
FileTransport: push-based p2p file transport for one-shot ODAGs.

Flow
----
send(payload):
    1. Write payload to this task's local hostPath output file.
    2. POST /push/<odag>/<task> to the local data-agent with the list of
       remote (cross-node) successors. The data-agent handles the transfer
       in a background goroutine and sets DataReady when done.
    3. Return immediately — the task pod can continue and exit without waiting.

recv(peer) / recv_all():
    Always a local file read. The odag-controller starts a task pod only
    after all upstream deps are DataReady, so the file is guaranteed present.

close():
    Sets state=Done and returns. The pod exits immediately regardless of
    whether the data-agent transfer is still in progress (option-3 decoupled
    transfer). The data-agent completes the push independently from the
    local file on the hostPath.

State protocol
--------------
Tasks signal their state to the local data-agent (reachable at DSF_NODE_IP:
8081) via PUT /state/<odag>/<task>. The controller queries this endpoint to
decide when to start downstream tasks (DataReady trigger, not PodSucceeded).

States set by the SDK:
    Executing   — on FileTransport.__init__()
    Done        — on close()

States set by the data-agent (after /push/ completes):
    DataReady   — all remote pushes succeeded
    Failed      — one or more pushes failed after retries

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

import json
import os
import urllib.request


_DATA_AGENT_PORT = 8081


class FileTransport:
    """
    File-based transport for one-shot (ODAG) task graphs.

    send(payload)   — write output locally and hand off remote pushes to the
                      data-agent; returns immediately (non-blocking).
    recv(peer)      — read a specific upstream dep's output (local file).
    recv_all()      — read all deps; returns {dep_name: bytes}.
    close()         — signal Done and exit; transfer continues in data-agent.
    """

    def __init__(self) -> None:
        self.odag_name: str = os.environ["DSF_ODAG_NAME"]
        self.task_name: str = os.environ["DSF_TASK_NAME"]
        self.output_dir: str = os.environ["DSF_OUTPUT_DIR"]
        self.node_name: str = os.environ.get("NODE_NAME", "")
        self.node_ip: str = os.environ.get("DSF_NODE_IP", "")
        self._set_state("Executing")

    # ------------------------------------------------------------------ #
    # state protocol                                                        #
    # ------------------------------------------------------------------ #

    def _set_state(self, state: str) -> None:
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
        Write payload locally and ask the data-agent to push to remote successors.
        Returns immediately — the data-agent handles the transfer independently.
        """
        # 1. Write to local hostPath (covers same-node successors and our own storage).
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, "output")
        with open(output_path, "wb") as f:
            f.write(payload)
        print(
            f"[{self.task_name}] wrote {len(payload)} bytes to {output_path}",
            flush=True,
        )

        # Signal DataReady on this node immediately after the local write.
        # Same-node successors can now be scheduled without waiting for any
        # cross-node transfer. Cross-node successors get their own DataReady
        # signal on their node when the data-agent push arrives there.
        self._set_state("DataReady")

        # 2. Build list of cross-node successors for the data-agent to push to.
        succs_env = os.environ.get("DSF_SUCCESSORS", "")
        successors = []
        for succ in [s for s in succs_env.split(",") if s]:
            succ_key = succ.upper().replace("-", "_")
            succ_node = os.environ.get(f"DSF_SUCC_{succ_key}_NODE", "")
            succ_host = os.environ.get(f"DSF_SUCC_{succ_key}_HOST", "")
            if succ_node == self.node_name:
                continue  # same-node: file already present on shared hostPath
            if not succ_host:
                print(
                    f"[{self.task_name}] WARNING: no host for successor {succ}; skipping",
                    flush=True,
                )
                continue
            successors.append({"name": succ, "host": succ_host})

        # 3. Hand off to data-agent (responds 200 immediately, pushes in background).
        self._request_push(successors)

    def _request_push(self, successors: list) -> None:
        if not self.node_ip:
            return
        url = f"http://{self.node_ip}:{_DATA_AGENT_PORT}/push/{self.odag_name}/{self.task_name}"
        body = json.dumps({"successors": successors}).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5):
                pass
            print(
                f"[{self.task_name}] handed off push to data-agent "
                f"({len(successors)} remote successor(s))",
                flush=True,
            )
        except Exception as e:
            print(f"[{self.task_name}] WARNING: failed to request push: {e}", flush=True)

    # ------------------------------------------------------------------ #
    # recv                                                                  #
    # ------------------------------------------------------------------ #

    def recv(self, peer: str | None = None) -> bytes:
        """
        Read the output of an upstream dependency from the local hostPath.
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
        """Read outputs from all upstream dependencies."""
        deps = [d for d in os.environ.get("DSF_DEPS", "").split(",") if d]
        if not deps:
            raise RuntimeError("recv_all() called but DSF_DEPS is empty")
        return {dep: self.recv(dep) for dep in deps}

    # ------------------------------------------------------------------ #
    # close                                                                 #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """
        Return immediately. The pod exits.
        The data-agent sets DataReady independently once the push completes.
        For leaf tasks (no send()), downstream scheduling uses PodSucceeded directly.
        """
        pass
