"""
Example DSF task — the four-function programming model.

This is the entire contract a user writes for a one-shot (ODAG) task.
The DSFTask object reads everything it needs from environment variables
that the odag-controller injected when it created this pod, so user code
never touches Kubernetes, ZMQ, or the data-agent directly.
"""

from dsf_sdk import DSFTask


# ---------------------------------------------------------------------------
# 1. INIT — instantiate DSFTask
# ---------------------------------------------------------------------------
# Reads injected env vars: DSF_TASK_NAME, DSF_DEPS, DSF_SUCCESSORS, NODE_NAME,
# DSF_RUNTIME, DSF_DATA_SIZE, etc. Selects the right transport (file for
# ODAG, pubsub for CDAG) automatically. Reports state="Executing" to the
# local data-agent.

task = DSFTask()


# ---------------------------------------------------------------------------
# 2. RECV — pull inputs from upstream tasks
# ---------------------------------------------------------------------------
# For ODAG (file transport): a local file read. The controller has already
# verified that every upstream's output is DataReady on this node before
# starting this pod, so the file is guaranteed present.

# Single upstream:
data = task.recv("upstream-task-name")

# Or, multiple upstreams at once:
inputs = task.recv_all()           # -> {"dep-a": ..., "dep-b": ...}

# Skip if this is a root task (task.is_root == True).


# ---------------------------------------------------------------------------
# 3. PROCESS — your task logic goes here
# ---------------------------------------------------------------------------
# Anything Python: ML inference, transforms, aggregations, file writes.
# DSF doesn't constrain or observe what happens in this block.

result = my_function(data)        # noqa: F821 — placeholder for user code


# ---------------------------------------------------------------------------
# 4. SEND — push outputs to downstream successors
# ---------------------------------------------------------------------------
# For ODAG (file transport): writes the payload to the local hostPath and
# asks the local data-agent to push it to every cross-node successor in the
# background. send() returns immediately; the pod can exit before the
# transfer completes — the data-agent owns the delivery from here.

task.send(result)

# Skip if this is a leaf task (task.is_leaf == True).


# ---------------------------------------------------------------------------
# 5. CLOSE — release resources
# ---------------------------------------------------------------------------
# Reports state="Done" to the data-agent and closes any open sockets / file
# handles. Safe to call even if send() was skipped.

task.close()
