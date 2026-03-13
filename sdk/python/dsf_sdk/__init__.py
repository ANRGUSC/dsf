"""
DSF Python SDK.

Usage in task images:

    from dsf_sdk import DSFTask

    task = DSFTask()
    data = task.recv("upstream-task-name")   # blocks until data arrives
    result = process(data)
    task.send("downstream-task-name", result)

The controller injects peer configuration as environment variables:
    DSF_TASK_NAME=<this-task-name>
    DSF_PEER_<NAME>=<transport>://<endpoint>

The SDK reads these env vars automatically.
"""

from dsf_sdk.api import DSFTask

__all__ = ["DSFTask"]
