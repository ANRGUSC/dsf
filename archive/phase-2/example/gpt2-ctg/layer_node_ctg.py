#!/usr/bin/env python3
"""
CTG full 12-layer GPT-2 with per-role weight loading.
Each pod loads ONLY the weights it needs; total weight RAM across all pods = ~475MB.
All qkv nodes are uniform (receive hidden_out from feeder or previous mlp-merge).
"""

import argparse
import io
import os
import time
from types import SimpleNamespace

import torch
import torch.nn as nn
import zmq
from transformers import GPT2Config
from transformers.models.gpt2.modeling_gpt2 import create_causal_mask, eager_attention_forward
from transformers.pytorch_utils import Conv1D

from gpt2_dag.tensor_dag import _run_attention, _split_qkv_by_heads

NUM_LAYERS = 12
SHARD_COUNT = 12
ZMQ_PORT = 5555
LINGER_MS = 1000
WEIGHTS_DIR = "/app/weights"


def _serialize(tensors: dict) -> bytes:
    buf = io.BytesIO()
    torch.save(tensors, buf)
    return buf.getvalue()


def _deserialize(data: bytes) -> dict:
    return torch.load(io.BytesIO(data), weights_only=False)


def _svc_addr(graph: str, task: str, ns: str) -> str:
    return f"tcp://{graph}-{task}-service.{ns}.svc.cluster.local:{ZMQ_PORT}"


def _make_pub(ctx: zmq.Context, port: int = ZMQ_PORT) -> zmq.Socket:
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.LINGER, LINGER_MS)
    sock.bind(f"tcp://*:{port}")
    return sock


def _make_sub(ctx: zmq.Context, endpoints: list) -> zmq.Socket:
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.LINGER, LINGER_MS)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    for ep in endpoints:
        print(f"  SUB connecting to {ep}")
        sock.connect(ep)
    return sock


def _pub_send(sock: zmq.Socket, topic: str, tensors: dict) -> None:
    sock.send_multipart([topic.encode(), _serialize(tensors)])


def _sub_recv(sock: zmq.Socket) -> tuple:
    topic_b, payload = sock.recv_multipart()
    return topic_b.decode(), _deserialize(payload)


def _load_config():
    return GPT2Config.from_pretrained(os.path.join(WEIGHTS_DIR, "config"))


def _load_qkv(layer_idx: int):
    """Load ln_1 + c_attn for one layer. All qkv nodes are uniform (no embeddings)."""
    config = _load_config()
    path = os.path.join(WEIGHTS_DIR, f"layer_{layer_idx}", "qkv.pt")
    d = torch.load(path, weights_only=True)
    ln_1 = nn.LayerNorm(config.n_embd)
    ln_1.weight.data = d["ln_1.weight"]
    ln_1.bias.data = d["ln_1.bias"]
    # Conv1D(nf, nx): weight shape [nx, nf]; c_attn is [768, 2304]
    c_attn = Conv1D(3 * config.n_embd, config.n_embd)
    c_attn.weight.data = d["c_attn.weight"]
    c_attn.bias.data = d["c_attn.bias"]
    split_size = config.n_embd
    head_dim = config.n_embd // config.n_head
    attn = SimpleNamespace(c_attn=c_attn, split_size=split_size, head_dim=head_dim)
    block = SimpleNamespace(ln_1=ln_1, attn=attn)
    return block, config


def _load_attn_shard(layer_idx: int):
    """No weights; only config for head_dim etc."""
    config = _load_config()
    head_dim = config.n_embd // config.n_head
    attn = SimpleNamespace(
        reorder_and_upcast_attn=False,
        attn_dropout=SimpleNamespace(p=0.0),
        training=False,
        scale_attn_weights=True,
    )
    block = SimpleNamespace(attn=attn)
    return block, config


def _load_attn_merge(layer_idx: int):
    config = _load_config()
    path = os.path.join(WEIGHTS_DIR, f"layer_{layer_idx}", "attn_merge.pt")
    d = torch.load(path, weights_only=True)
    c_proj = Conv1D(config.n_embd, config.n_embd)
    c_proj.weight.data = d["c_proj.weight"]
    c_proj.bias.data = d["c_proj.bias"]
    ln_2 = nn.LayerNorm(config.n_embd)
    ln_2.weight.data = d["ln_2.weight"]
    ln_2.bias.data = d["ln_2.bias"]
    attn = SimpleNamespace(c_proj=c_proj, resid_dropout=nn.Dropout(0.0))
    block = SimpleNamespace(attn=attn, ln_2=ln_2)
    return block, config


def _load_mlp_shard(layer_idx: int, shard_idx: int):
    """Pre-sliced c_fc: weight [768, 256], bias [256]. No full c_fc."""
    path = os.path.join(WEIGHTS_DIR, f"layer_{layer_idx}", f"mlp_shard_{shard_idx}.pt")
    d = torch.load(path, weights_only=True)
    # weight [nx, out_slice], bias [out_slice]
    w = d["weight"]
    b = d["bias"]
    block = SimpleNamespace(weight=w, bias=b, nf=w.shape[1])
    return block, None


def _load_mlp_merge(layer_idx: int):
    config = _load_config()
    path = os.path.join(WEIGHTS_DIR, f"layer_{layer_idx}", "mlp_merge.pt")
    d = torch.load(path, weights_only=True)
    # mlp.c_proj: in=4*n_embd (3072), out=n_embd (768)
    inner_dim = 4 * config.n_embd
    c_proj = Conv1D(config.n_embd, inner_dim)
    c_proj.weight.data = d["c_proj.weight"]
    c_proj.bias.data = d["c_proj.bias"]
    mlp = SimpleNamespace(c_proj=c_proj, dropout=nn.Dropout(0.0))
    block = SimpleNamespace(mlp=mlp)
    return block, config


# ----- role loops -----

def run_qkv(pub, sub, block, config, layer_idx):
    tag = f"l{layer_idx}-qkv"
    print(f"[{tag}] ready, waiting for hidden_out...")
    while True:
        _, msg = _sub_recv(sub)
        rid = msg["rid"]
        hidden = msg["hidden_out"]
        t0 = time.time()
        with torch.no_grad():
            seq_len = hidden.shape[1]
            cache_position = torch.arange(seq_len, device=hidden.device)
            position_ids = cache_position.unsqueeze(0)
            causal_mask = create_causal_mask(
                config=config, input_embeds=hidden,
                attention_mask=None, cache_position=cache_position,
                past_key_values=None, position_ids=position_ids,
            )
            residual = hidden
            ln1_out = block.ln_1(hidden)
            q, k, v = _split_qkv_by_heads(block.attn, ln1_out)
        dt = time.time() - t0
        _pub_send(pub, tag, {
            "rid": rid, "query": q, "key": k, "value": v,
            "residual": residual, "causal_mask": causal_mask,
        })
        print(f"[{tag}] rid={rid} compute={dt*1000:.1f}ms")


def run_attn_shard(pub, sub, block, config, layer_idx, shard_idx):
    tag = f"l{layer_idx}-attn-shard-{shard_idx}"
    heads_per_shard = config.n_head // SHARD_COUNT
    hs, he = shard_idx * heads_per_shard, (shard_idx + 1) * heads_per_shard
    print(f"[{tag}] ready, waiting for qkv...")
    while True:
        _, msg = _sub_recv(sub)
        rid = msg["rid"]
        t0 = time.time()
        with torch.no_grad():
            ctx, _ = _run_attention(
                block.attn,
                msg["query"][:, hs:he], msg["key"][:, hs:he], msg["value"][:, hs:he],
                msg["causal_mask"],
            )
        dt = time.time() - t0
        _pub_send(pub, tag, {"rid": rid, "shard_idx": shard_idx, "ctx_shard": ctx})
        print(f"[{tag}] rid={rid} compute={dt*1000:.1f}ms")


def run_attn_merge(pub, sub, block, config, layer_idx):
    tag = f"l{layer_idx}-attn-merge"
    qkv_topic = f"l{layer_idx}-qkv"
    pending = {}
    print(f"[{tag}] ready, waiting for {qkv_topic} + {SHARD_COUNT} attn-shards...")
    while True:
        topic, msg = _sub_recv(sub)
        rid = msg["rid"]
        if rid not in pending:
            pending[rid] = {"shards": {}}
        if topic == qkv_topic:
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
        _pub_send(pub, tag, {"rid": rid, "hidden_after_attn": hidden_after_attn, "ln2": ln2})
        print(f"[{tag}] rid={rid} compute={dt*1000:.1f}ms")


def run_mlp_shard(pub, sub, block, layer_idx, shard_idx):
    tag = f"l{layer_idx}-mlp-shard-{shard_idx}"
    print(f"[{tag}] ready, waiting for attn-merge...")
    while True:
        _, msg = _sub_recv(sub)
        rid = msg["rid"]
        x = msg["ln2"]
        t0 = time.time()
        with torch.no_grad():
            # Pre-sliced: block.weight [768, 256], block.bias [256]
            size_out = x.size()[:-1] + (block.nf,)
            y = torch.addmm(block.bias, x.reshape(-1, x.size(-1)), block.weight)
            y = y.view(size_out)
            shard_act = torch.nn.functional.gelu(y)
        dt = time.time() - t0
        _pub_send(pub, tag, {"rid": rid, "shard_idx": shard_idx, "mlp_shard": shard_act})
        print(f"[{tag}] rid={rid} compute={dt*1000:.1f}ms")


def run_mlp_merge(pub, sub, block, config, layer_idx):
    tag = f"l{layer_idx}-mlp-merge"
    attn_merge_topic = f"l{layer_idx}-attn-merge"
    pending = {}
    print(f"[{tag}] ready, waiting for {attn_merge_topic} + {SHARD_COUNT} mlp-shards...")
    while True:
        topic, msg = _sub_recv(sub)
        rid = msg["rid"]
        if rid not in pending:
            pending[rid] = {"shards": {}}
        if topic == attn_merge_topic:
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
        _pub_send(pub, tag, {"rid": rid, "hidden_out": hidden_out})
        print(f"[{tag}] rid={rid} shape={hidden_out.shape} compute={dt*1000:.1f}ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--role", required=True,
                   choices=["qkv", "attn-shard", "attn-merge", "mlp-shard", "mlp-merge"])
    p.add_argument("--layer", type=int, required=True)
    p.add_argument("--shard-idx", type=int, default=0)
    p.add_argument("--graph-name", default=os.environ.get("ZMQ_GRAPH_NAME", "gpt2-ctg"))
    p.add_argument("--namespace", default=os.environ.get("ZMQ_NAMESPACE", "default"))
    args = p.parse_args()

    g, ns = args.graph_name, args.namespace
    L = args.layer
    svc = lambda task: _svc_addr(g, task, ns)

    print(f"=== {args.role} layer={L} (shard_idx={args.shard_idx}) starting ===")
    t0 = time.time()
    if args.role == "qkv":
        block, config = _load_qkv(L)
    elif args.role == "attn-shard":
        block, config = _load_attn_shard(L)
    elif args.role == "attn-merge":
        block, config = _load_attn_merge(L)
    elif args.role == "mlp-shard":
        block, config = _load_mlp_shard(L, args.shard_idx), None
    else:
        block, config = _load_mlp_merge(L)
    print(f"  weights loaded in {time.time()-t0:.2f}s")

    ctx = zmq.Context()
    pub = _make_pub(ctx, ZMQ_PORT)

    if args.role == "qkv":
        upstream = svc("feeder") if L == 0 else svc(f"l{L-1}-mlp-merge")
        sub = _make_sub(ctx, [upstream])
        time.sleep(5)
        run_qkv(pub, sub, block, config, L)
    elif args.role == "attn-shard":
        sub = _make_sub(ctx, [svc(f"l{L}-qkv")])
        time.sleep(5)
        run_attn_shard(pub, sub, block, config, L, args.shard_idx)
    elif args.role == "attn-merge":
        endpoints = [svc(f"l{L}-qkv")] + [svc(f"l{L}-attn-shard-{i}") for i in range(SHARD_COUNT)]
        sub = _make_sub(ctx, endpoints)
        time.sleep(5)
        run_attn_merge(pub, sub, block, config, L)
    elif args.role == "mlp-shard":
        sub = _make_sub(ctx, [svc(f"l{L}-attn-merge")])
        time.sleep(5)
        run_mlp_shard(pub, sub, block, L, args.shard_idx)
    else:
        endpoints = [svc(f"l{L}-attn-merge")] + [svc(f"l{L}-mlp-shard-{i}") for i in range(SHARD_COUNT)]
        sub = _make_sub(ctx, endpoints)
        time.sleep(5)
        run_mlp_merge(pub, sub, block, config, L)


if __name__ == "__main__":
    main()
