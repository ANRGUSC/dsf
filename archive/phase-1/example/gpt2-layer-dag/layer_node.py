#!/usr/bin/env python3
"""
GPT-2 single-layer tensor-DAG node.

One invocation = one DAG node.  Roles:
  qkv          – embed random input, compute Q/K/V          (root)
  attn-shard   – run attention on one head-group shard      (depends on qkv)
  attn-merge   – concat attention shards, c_proj, residual  (depends on all attn-shards + qkv)
  mlp-shard    – run MLP on one column shard                (depends on attn-merge)
  mlp-merge    – concat MLP shards, c_proj, residual        (depends on all mlp-shards + attn-merge)

Tensor data is saved to /data/dag-outputs/<dag>/<step>/data.pt on each node's
hostPath.  Cross-node reads go through the Data Agent HTTP file server
(port 8080 on each node).  The controller sets DEP_<STEP>_NODE env vars so
we know where each dependency ran.
"""

import time as _time
_T_PROC_START = _time.time()

import argparse
import io
import os
import tempfile
import urllib.request

import torch
from transformers import GPT2Config, GPT2LMHeadModel
from transformers.models.gpt2.modeling_gpt2 import create_causal_mask

from gpt2_dag.tensor_dag import (
    GPT2TensorDAGLMHeadModel,
    _conv1d_output_slice,
    _run_attention,
    _split_qkv_by_heads,
)

_T_IMPORTS_DONE = _time.time()

SHARD_COUNT = 12
SEED = 42
BATCH, SEQLEN = 1, 8

CONFIG = GPT2Config(
    n_layer=1,
    n_embd=768,
    n_head=12,
    n_inner=3072,
    vocab_size=50257,
    n_positions=1024,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _load_model() -> GPT2TensorDAGLMHeadModel:
    """Deterministic 1-layer GPT-2 DAG model (same weights in every pod)."""
    torch.manual_seed(SEED)
    hf = GPT2LMHeadModel(CONFIG)
    dag = GPT2TensorDAGLMHeadModel.from_hf_model(hf, shard_count=SHARD_COUNT)
    dag.eval()
    return dag


def _data_path(dag_name: str, step: str) -> str:
    return f"/data/dag-outputs/{dag_name}/{step}/data.pt"


def _save(dag_name: str, step: str, tensors: dict) -> None:
    path = _data_path(dag_name, step)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    torch.save(tensors, path)
    print(f"[{step}] saved  {path}  keys={list(tensors)}")


DATA_AGENT_PORT = 8080

CLUSTER_NODES = os.environ.get(
    "CLUSTER_NODES",
    "anrg-1,anrg-3,anrg-4,anrg-5,anrg-6,anrg-7,anrg-8,anrg-9",
).split(",")


def _load(dag_name: str, step: str) -> dict:
    local = _data_path(dag_name, step)
    if os.path.exists(local):
        d = torch.load(local, weights_only=False)
        print(f"  loaded {step} from local {local}")
        return d

    for node in CLUSTER_NODES:
        url = f"http://{node}:{DATA_AGENT_PORT}/{dag_name}/{step}/data.pt"
        try:
            resp = urllib.request.urlopen(url, timeout=5)
            buf = io.BytesIO(resp.read())
            d = torch.load(buf, weights_only=False)
            print(f"  loaded {step} from {node} ({len(buf.getvalue())} bytes)")
            return d
        except Exception:
            continue

    raise FileNotFoundError(
        f"{local} not found locally and no data agent on "
        f"{CLUSTER_NODES} has {dag_name}/{step}/data.pt"
    )


# ── roles ────────────────────────────────────────────────────────────────────

def run_qkv(dag_name: str) -> dict:
    t0 = _time.time()
    model = _load_model()
    block = model.transformer.h[0]
    t_model = _time.time()

    t_data = t_model  # qkv has no deps to fetch

    torch.manual_seed(SEED + 1)
    input_ids = torch.randint(0, CONFIG.vocab_size, (BATCH, SEQLEN))
    inputs_embeds = model.transformer.wte(input_ids)
    cache_position = torch.arange(SEQLEN)
    position_ids = cache_position.unsqueeze(0)
    hidden = inputs_embeds + model.transformer.wpe(position_ids)
    hidden = model.transformer.drop(hidden)

    causal_mask = create_causal_mask(
        config=model.config,
        input_embeds=inputs_embeds,
        attention_mask=None,
        cache_position=cache_position,
        past_key_values=None,
        position_ids=position_ids,
    )

    residual = hidden
    ln1 = block.ln_1(hidden)
    q, k, v = _split_qkv_by_heads(block.attn, ln1)
    t_compute = _time.time()

    _save(dag_name, "qkv", {
        "query": q, "key": k, "value": v,
        "residual": residual, "causal_mask": causal_mask,
    })
    t_save = _time.time()

    return {"model_load": t_model - t0, "data_fetch": t_data - t_model,
            "compute": t_compute - t_data, "save": t_save - t_compute}


def run_attn_shard(dag_name: str, idx: int) -> dict:
    t0 = _time.time()
    model = _load_model()
    block = model.transformer.h[0]
    t_model = _time.time()

    qkv = _load(dag_name, "qkv")
    t_data = _time.time()

    heads_per_shard = CONFIG.n_head // SHARD_COUNT
    hs, he = idx * heads_per_shard, (idx + 1) * heads_per_shard
    ctx, _ = _run_attention(
        block.attn,
        qkv["query"][:, hs:he],
        qkv["key"][:, hs:he],
        qkv["value"][:, hs:he],
        qkv["causal_mask"],
    )
    t_compute = _time.time()

    _save(dag_name, f"attn-shard-{idx}", {"ctx_shard": ctx})
    t_save = _time.time()

    return {"model_load": t_model - t0, "data_fetch": t_data - t_model,
            "compute": t_compute - t_data, "save": t_save - t_compute}


def run_attn_merge(dag_name: str) -> dict:
    t0 = _time.time()
    model = _load_model()
    block = model.transformer.h[0]
    t_model = _time.time()

    ctx_parts = [_load(dag_name, f"attn-shard-{i}")["ctx_shard"] for i in range(SHARD_COUNT)]
    residual = _load(dag_name, "qkv")["residual"]
    t_data = _time.time()

    attn_ctx = torch.cat(ctx_parts, dim=2)
    attn_ctx = attn_ctx.reshape(*attn_ctx.shape[:-2], -1).contiguous()
    attn_out = block.attn.c_proj(attn_ctx)
    attn_out = block.attn.resid_dropout(attn_out)
    hidden_after_attn = residual + attn_out

    ln2 = block.ln_2(hidden_after_attn)
    t_compute = _time.time()

    _save(dag_name, "attn-merge", {"hidden_after_attn": hidden_after_attn, "ln2": ln2})
    t_save = _time.time()

    return {"model_load": t_model - t0, "data_fetch": t_data - t_model,
            "compute": t_compute - t_data, "save": t_save - t_compute}


def run_mlp_shard(dag_name: str, idx: int) -> dict:
    t0 = _time.time()
    model = _load_model()
    block = model.transformer.h[0]
    t_model = _time.time()

    ln2 = _load(dag_name, "attn-merge")["ln2"]
    t_data = _time.time()

    inner_dim = block.mlp.c_fc.nf
    chunk = inner_dim // SHARD_COUNT
    start, end = idx * chunk, (idx + 1) * chunk
    shard_act = block.mlp.act(_conv1d_output_slice(block.mlp.c_fc, ln2, start, end))
    t_compute = _time.time()

    _save(dag_name, f"mlp-shard-{idx}", {"mlp_shard": shard_act})
    t_save = _time.time()

    return {"model_load": t_model - t0, "data_fetch": t_data - t_model,
            "compute": t_compute - t_data, "save": t_save - t_compute}


def run_mlp_merge(dag_name: str) -> dict:
    t0 = _time.time()
    model = _load_model()
    block = model.transformer.h[0]
    t_model = _time.time()

    mlp_parts = [_load(dag_name, f"mlp-shard-{i}")["mlp_shard"] for i in range(SHARD_COUNT)]
    residual2 = _load(dag_name, "attn-merge")["hidden_after_attn"]
    t_data = _time.time()

    ff = block.mlp.c_proj(torch.cat(mlp_parts, dim=-1))
    ff = block.mlp.dropout(ff)
    hidden_out = residual2 + ff
    t_compute = _time.time()

    _save(dag_name, "mlp-merge", {"hidden_out": hidden_out})
    t_save = _time.time()

    print(f"[mlp-merge] final output shape: {hidden_out.shape}")
    return {"model_load": t_model - t0, "data_fetch": t_data - t_model,
            "compute": t_compute - t_data, "save": t_save - t_compute}


# ── main ─────────────────────────────────────────────────────────────────────

ROLES = {
    "qkv":        lambda a: run_qkv(a.dag_name),
    "attn-shard": lambda a: run_attn_shard(a.dag_name, a.shard_idx),
    "attn-merge": lambda a: run_attn_merge(a.dag_name),
    "mlp-shard":  lambda a: run_mlp_shard(a.dag_name, a.shard_idx),
    "mlp-merge":  lambda a: run_mlp_merge(a.dag_name),
}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--role", required=True, choices=list(ROLES))
    p.add_argument("--shard-idx", type=int, default=0)
    p.add_argument("--dag-name", default="gpt2-layer-dag")
    args = p.parse_args()

    import_time = _T_IMPORTS_DONE - _T_PROC_START
    print(f"=== {args.role} (shard_idx={args.shard_idx}) ===")

    with torch.no_grad():
        timings = ROLES[args.role](args)

    total = _time.time() - _T_PROC_START
    runtime = timings["model_load"] + timings["data_fetch"] + timings["compute"] + timings["save"]

    print(f"=== {args.role} done ===")
    print(f"PROFILE {args.role} | "
          f"import={import_time:.3f}s "
          f"model_load={timings['model_load']:.3f}s "
          f"data_fetch={timings['data_fetch']:.3f}s "
          f"compute={timings['compute']:.3f}s "
          f"save={timings['save']:.3f}s "
          f"runtime={runtime:.3f}s "
          f"total={total:.3f}s")
