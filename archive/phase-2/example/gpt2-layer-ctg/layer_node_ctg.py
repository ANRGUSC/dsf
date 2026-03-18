#!/usr/bin/env python3
"""
CTG (Continuous Task Graph) version of the GPT-2 single-layer tensor-DAG.

Each role is a long-running process:
  - Loads the model ONCE at startup
  - Listens for input tensors on ZeroMQ SUB socket(s)
  - Computes its piece of the layer
  - Publishes output tensors on a ZeroMQ PUB socket

ZeroMQ topology (PUB/SUB):
  feeder ──PUB──▶ qkv ──PUB──▶ attn-shard-{0..11} ──PUB──▶ attn-merge ──PUB──▶
  mlp-shard-{0..11} ──PUB──▶ mlp-merge ──PUB──▶ feeder (result)

Merge nodes subscribe to ALL upstream shards and collect per-request_id.
"""

import argparse
import io
import os
import time

import torch
import zmq
from transformers import GPT2LMHeadModel
from transformers.models.gpt2.modeling_gpt2 import create_causal_mask

from gpt2_dag.tensor_dag import (
    GPT2TensorDAGLMHeadModel,
    _conv1d_output_slice,
    _run_attention,
    _split_qkv_by_heads,
)

SHARD_COUNT = 12
ZMQ_PORT = 5555
LINGER_MS = 1000
MODEL_CACHE = "/app/model_cache"


# ── serialization ────────────────────────────────────────────────────────────

def _serialize(tensors: dict) -> bytes:
    buf = io.BytesIO()
    torch.save(tensors, buf)
    return buf.getvalue()


def _deserialize(data: bytes) -> dict:
    return torch.load(io.BytesIO(data), weights_only=False)


# ── ZeroMQ helpers ───────────────────────────────────────────────────────────

def _svc_addr(graph: str, task: str, ns: str) -> str:
    return f"tcp://{graph}-{task}-service.{ns}.svc.cluster.local:{ZMQ_PORT}"


def _make_pub(ctx: zmq.Context, port: int = ZMQ_PORT) -> zmq.Socket:
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.LINGER, LINGER_MS)
    sock.bind(f"tcp://*:{port}")
    return sock


def _make_sub(ctx: zmq.Context, endpoints: list[str]) -> zmq.Socket:
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.LINGER, LINGER_MS)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    for ep in endpoints:
        print(f"  SUB connecting to {ep}")
        sock.connect(ep)
    return sock


def _pub_send(sock: zmq.Socket, topic: str, tensors: dict) -> None:
    sock.send_multipart([topic.encode(), _serialize(tensors)])


def _sub_recv(sock: zmq.Socket) -> tuple[str, dict]:
    topic_b, payload = sock.recv_multipart()
    return topic_b.decode(), _deserialize(payload)


# ── model loading (once) ─────────────────────────────────────────────────────

def _load_model():
    print("  loading pretrained GPT-2 weights...")
    hf = GPT2LMHeadModel.from_pretrained("gpt2", cache_dir=MODEL_CACHE)
    dag = GPT2TensorDAGLMHeadModel.from_hf_model(hf, shard_count=SHARD_COUNT)
    dag.eval()
    return dag


# ── role loops ───────────────────────────────────────────────────────────────

def run_qkv(pub, sub, model):
    block = model.transformer.h[0]
    print("[qkv] ready, waiting for input...")
    while True:
        _, msg = _sub_recv(sub)
        rid, input_ids = msg["rid"], msg["input_ids"]
        t0 = time.time()
        with torch.no_grad():
            inputs_embeds = model.transformer.wte(input_ids)
            cache_position = torch.arange(input_ids.shape[1])
            position_ids = cache_position.unsqueeze(0)
            hidden = inputs_embeds + model.transformer.wpe(position_ids)
            hidden = model.transformer.drop(hidden)
            causal_mask = create_causal_mask(
                config=model.config, input_embeds=inputs_embeds,
                attention_mask=None, cache_position=cache_position,
                past_key_values=None, position_ids=position_ids,
            )
            residual = hidden
            ln1 = block.ln_1(hidden)
            q, k, v = _split_qkv_by_heads(block.attn, ln1)
        dt = time.time() - t0
        _pub_send(pub, "qkv", {
            "rid": rid, "query": q, "key": k, "value": v,
            "residual": residual, "causal_mask": causal_mask,
        })
        print(f"[qkv] rid={rid} compute={dt*1000:.1f}ms")


def run_attn_shard(pub, sub, model, idx):
    block = model.transformer.h[0]
    topic = f"attn-shard-{idx}"
    print(f"[{topic}] ready, waiting for qkv...")
    heads_per_shard = model.config.n_head // SHARD_COUNT
    hs, he = idx * heads_per_shard, (idx + 1) * heads_per_shard
    while True:
        _, msg = _sub_recv(sub)
        rid = msg["rid"]
        t0 = time.time()
        with torch.no_grad():
            ctx, _ = _run_attention(
                block.attn,
                msg["query"][:, hs:he], msg["key"][:, hs:he],
                msg["value"][:, hs:he], msg["causal_mask"],
            )
        dt = time.time() - t0
        _pub_send(pub, topic, {"rid": rid, "shard_idx": idx, "ctx_shard": ctx})
        print(f"[{topic}] rid={rid} compute={dt*1000:.1f}ms")


def run_attn_merge(pub, sub, model):
    block = model.transformer.h[0]
    pending = {}
    print("[attn-merge] ready, waiting for qkv + 12 attn-shards...")
    while True:
        topic, msg = _sub_recv(sub)
        rid = msg["rid"]
        if rid not in pending:
            pending[rid] = {"shards": {}}

        if topic == "qkv":
            pending[rid]["residual"] = msg["residual"]
        else:
            pending[rid]["shards"][msg["shard_idx"]] = msg["ctx_shard"]

        if "residual" not in pending[rid] or len(pending[rid]["shards"]) < SHARD_COUNT:
            continue

        parts = pending.pop(rid)
        t0 = time.time()
        with torch.no_grad():
            ctx_list = [parts["shards"][i] for i in range(SHARD_COUNT)]
            attn_ctx = torch.cat(ctx_list, dim=2)
            attn_ctx = attn_ctx.reshape(*attn_ctx.shape[:-2], -1).contiguous()
            attn_out = block.attn.c_proj(attn_ctx)
            attn_out = block.attn.resid_dropout(attn_out)
            hidden_after_attn = parts["residual"] + attn_out
            ln2 = block.ln_2(hidden_after_attn)
        dt = time.time() - t0
        _pub_send(pub, "attn-merge", {
            "rid": rid, "hidden_after_attn": hidden_after_attn, "ln2": ln2,
        })
        print(f"[attn-merge] rid={rid} compute={dt*1000:.1f}ms")


def run_mlp_shard(pub, sub, model, idx):
    block = model.transformer.h[0]
    topic = f"mlp-shard-{idx}"
    inner_dim = block.mlp.c_fc.nf
    chunk = inner_dim // SHARD_COUNT
    start, end = idx * chunk, (idx + 1) * chunk
    print(f"[{topic}] ready, waiting for attn-merge...")
    while True:
        _, msg = _sub_recv(sub)
        rid = msg["rid"]
        t0 = time.time()
        with torch.no_grad():
            shard_act = block.mlp.act(
                _conv1d_output_slice(block.mlp.c_fc, msg["ln2"], start, end)
            )
        dt = time.time() - t0
        _pub_send(pub, topic, {"rid": rid, "shard_idx": idx, "mlp_shard": shard_act})
        print(f"[{topic}] rid={rid} compute={dt*1000:.1f}ms")


def run_mlp_merge(pub, sub, model):
    block = model.transformer.h[0]
    pending = {}
    print("[mlp-merge] ready, waiting for attn-merge + 12 mlp-shards...")
    while True:
        topic, msg = _sub_recv(sub)
        rid = msg["rid"]
        if rid not in pending:
            pending[rid] = {"shards": {}}

        if topic == "attn-merge":
            pending[rid]["hidden_after_attn"] = msg["hidden_after_attn"]
        else:
            pending[rid]["shards"][msg["shard_idx"]] = msg["mlp_shard"]

        if "hidden_after_attn" not in pending[rid] or len(pending[rid]["shards"]) < SHARD_COUNT:
            continue

        parts = pending.pop(rid)
        t0 = time.time()
        with torch.no_grad():
            mlp_list = [parts["shards"][i] for i in range(SHARD_COUNT)]
            ff = block.mlp.c_proj(torch.cat(mlp_list, dim=-1))
            ff = block.mlp.dropout(ff)
            hidden_out = parts["hidden_after_attn"] + ff
        dt = time.time() - t0
        _pub_send(pub, "mlp-merge", {"rid": rid, "hidden_out": hidden_out})
        print(f"[mlp-merge] rid={rid} shape={hidden_out.shape} compute={dt*1000:.1f}ms")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--role", required=True,
                   choices=["qkv", "attn-shard", "attn-merge",
                            "mlp-shard", "mlp-merge"])
    p.add_argument("--shard-idx", type=int, default=0)
    p.add_argument("--graph-name", default=os.environ.get("ZMQ_GRAPH_NAME", "gpt2-ctg"))
    p.add_argument("--namespace", default=os.environ.get("ZMQ_NAMESPACE", "default"))
    args = p.parse_args()

    g, ns = args.graph_name, args.namespace
    svc = lambda task: _svc_addr(g, task, ns)

    print(f"=== {args.role} (shard_idx={args.shard_idx}) starting ===")
    t0 = time.time()
    model = _load_model()
    print(f"  model loaded in {time.time()-t0:.2f}s")

    ctx = zmq.Context()
    pub = _make_pub(ctx, ZMQ_PORT)

    if args.role == "qkv":
        sub = _make_sub(ctx, [svc("feeder")])
        time.sleep(5)
        run_qkv(pub, sub, model)

    elif args.role == "attn-shard":
        sub = _make_sub(ctx, [svc("qkv")])
        time.sleep(5)
        run_attn_shard(pub, sub, model, args.shard_idx)

    elif args.role == "attn-merge":
        endpoints = [svc("qkv")] + [svc(f"attn-shard-{i}") for i in range(SHARD_COUNT)]
        sub = _make_sub(ctx, endpoints)
        time.sleep(5)
        run_attn_merge(pub, sub, model)

    elif args.role == "mlp-shard":
        sub = _make_sub(ctx, [svc("attn-merge")])
        time.sleep(5)
        run_mlp_shard(pub, sub, model, args.shard_idx)

    elif args.role == "mlp-merge":
        endpoints = [svc("attn-merge")] + [svc(f"mlp-shard-{i}") for i in range(SHARD_COUNT)]
        sub = _make_sub(ctx, endpoints)
        time.sleep(5)
        run_mlp_merge(pub, sub, model)


if __name__ == "__main__":
    main()
