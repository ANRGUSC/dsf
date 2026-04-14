# Benchmark toggles

The default cluster is configured for **direct P2P transports**:

- **ODAG**: per-node hostPath writes + data-agent push over HTTP (`DSF_TRANSPORT_PATTERN=file`)
- **CDAG**: ZeroMQ PUB/SUB directly between pods (`DSF_TRANSPORT_PATTERN=pubsub`)

For benchmarks that need a centralized baseline, enable one of the scripts
below. Each `enable-*.sh` is idempotent; run the matching `disable-*.sh`
before switching back to defaults.

## NFS baseline for ODAG

`enable-nfs.sh` — mounts `nfs-server:/data/nfs-export` over `/data/dsf-outputs`
on every worker node via a privileged DaemonSet. All ODAG tasks then read/write
through the shared NFS mount (the per-node data-agent pushes become redundant
because the file is visible on every node the moment it's written).

`disable-nfs.sh` — unmounts NFS on all nodes, deletes the overlay DaemonSet,
and restarts data-agents so they see local disk again. The `nfs-server`
Deployment + Service are left running so re-enabling is fast.

**WARNING:** the NFS overlay silently changes ODAG behaviour: actual network
flows become phantom redundant pushes because the file is already visible via
NFS. Only enable during explicit NFS-vs-P2P benchmark runs.

## MQTT baseline for CDAG

MQTT is selected per-template via env vars on task specs, not by a cluster-wide
switch. The `mqtt-broker` Deployment + Service are always running in
`dsf-system`; they're idle until a task sets `DSF_TRANSPORT_PATTERN=mqtt`.

`enable-mqtt.sh` — no cluster changes; just prints the per-template env var
patch you need to apply to switch a CDAG template to MQTT.

`disable-mqtt.sh` — symmetric: prints the reverse patch.

To stop paying the broker's idle cost entirely:
`kubectl delete deploy,svc -n dsf-system mqtt-broker`

## Files

- `enable-nfs.sh` / `disable-nfs.sh` — NFS overlay for ODAG
- `enable-mqtt.sh` / `disable-mqtt.sh` — MQTT per-template guidance
- `status.sh` — print which benchmark modes are currently active
