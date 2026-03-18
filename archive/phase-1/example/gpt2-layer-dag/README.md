# GPT-2 single-layer tensor-DAG on DSF

Runs one transformer layer of [gpt2-dag](https://github.com/ANRGUSC/gpt2-dag)
as a **27-node DSF DAG** matching the Sh=12 tensor-sharded structure:

```
qkv ──┬── attn-shard-0  ──┐
      ├── attn-shard-1  ──┤
      ├── ...           ──┤
      └── attn-shard-11 ──┴── attn-merge ──┬── mlp-shard-0  ──┐
                                           ├── mlp-shard-1  ──┤
                                           ├── ...           ──┤
                                           └── mlp-shard-11 ──┴── mlp-merge
```

Each node runs in its own pod.  Tensor data flows between pods via a shared
`hostPath` volume at `/data/dag-outputs/`.

## Prerequisites

- `gpt2-dag` cloned next to `dsf` (e.g. `~/gpt2-dag` and `~/dsf`).
- Phase-1 CRD installed and a DAG controller running (random-controller or heft-controller).
- Single-node cluster (Kind, Minikube, …) or all pods pinned to one node, because
  tensor data is exchanged via the shared hostPath.

## Build

```bash
cd ~                                            # parent of dsf/ and gpt2-dag/
bash dsf/phase-1/example/gpt2-layer-dag/build.sh
```

This builds one Docker image and tags it as:
`gpt2-single-layer:{qkv, attn-shard, attn-merge, mlp-shard, mlp-merge}`.

## Run

```bash
kubectl apply -f dsf/phase-1/example/gpt2-layer-dag/dag.yml
kubectl get pods -l dag-name=gpt2-layer-dag -w
```

Watch the DAG execute: `qkv` runs first, then 12 `attn-shard-*` pods in
parallel, then `attn-merge`, then 12 `mlp-shard-*` in parallel, then
`mlp-merge`.

```bash
kubectl logs gpt2-layer-dag-mlp-merge    # should print final output shape
```

## How data flows

| Step | Reads from | Writes to |
|------|-----------|-----------|
| `qkv` | (generates random input) | Q, K, V, residual, causal_mask |
| `attn-shard-i` | qkv output | context shard for heads `[i*1 : (i+1)*1]` |
| `attn-merge` | 12 attn-shard outputs + qkv residual | hidden_after_attn, ln2 |
| `mlp-shard-i` | attn-merge ln2 | MLP activation shard |
| `mlp-merge` | 12 mlp-shard outputs + attn-merge residual | final hidden_states |

All tensors are saved as `.pt` files under `/data/dag-outputs/gpt2-layer-dag/<step>/data.pt`.
Each pod deterministically initialises the same 1-layer GPT-2 weights (fixed seed) so no
weight-sharing volume is needed.

## Files

| File | Purpose |
|------|---------|
| `layer_node.py` | Single script, role selected by `--role` arg |
| `Dockerfile` | One image with torch + transformers + gpt2_dag |
| `dag.yml` | 27-step DSF DAG |
| `build.sh` | Build image and tag for each role |
