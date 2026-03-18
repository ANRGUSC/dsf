#!/usr/bin/env python3
"""
Feeder for GPT-2 CTG with per-role weights (configurable layer count).
Owns wte + wpe + ln_f only (~150MB). Does embedding (input_ids -> hidden) and
decoding (hidden -> ln_f -> wte.T -> logits). Sends/receives hidden_out only.
"""

import argparse
import io
import os
import time

import torch
import torch.nn as nn
import zmq
from transformers import GPT2Config, GPT2Tokenizer

NUM_LAYERS = 6  # must match generate_ctg.py; 6 = half of 12
ZMQ_PORT = 5555
LINGER_MS = 1000
WEIGHTS_DIR = "/app/weights"


def _serialize(tensors: dict) -> bytes:
    buf = io.BytesIO()
    torch.save(tensors, buf)
    return buf.getvalue()


def _deserialize(data: bytes) -> dict:
    return torch.load(io.BytesIO(data), weights_only=False)


def _svc_addr(task: str) -> str:
    graph = os.environ.get("ZMQ_GRAPH_NAME", "gpt2-ctg")
    ns = os.environ.get("ZMQ_NAMESPACE", "default")
    return f"tcp://{graph}-{task}-service.{ns}.svc.cluster.local:{ZMQ_PORT}"


def _setup_sockets(ctx):
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.LINGER, LINGER_MS)
    pub.bind(f"tcp://*:{ZMQ_PORT}")
    print(f"[feeder] PUB bound on port {ZMQ_PORT}")
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, LINGER_MS)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    sub.connect(_svc_addr(f"l{NUM_LAYERS-1}-mlp-merge"))
    print(f"[feeder] SUB connected to l{NUM_LAYERS-1}-mlp-merge")
    return pub, sub


def _load_feeder_weights():
    """Load only wte, wpe, ln_f (~150MB). lm_head = wte.weight (tied)."""
    config = GPT2Config.from_pretrained(os.path.join(WEIGHTS_DIR, "config"))
    path = os.path.join(WEIGHTS_DIR, "feeder.pt")
    d = torch.load(path, weights_only=True)
    vocab_size, n_embd = d["wte.weight"].shape
    max_pos = d["wpe.weight"].shape[0]
    wte = nn.Embedding(vocab_size, n_embd)
    wte.weight.data = d["wte.weight"]
    wpe = nn.Embedding(max_pos, n_embd)
    wpe.weight.data = d["wpe.weight"]
    ln_f = nn.LayerNorm(n_embd)
    ln_f.weight.data = d["ln_f.weight"]
    ln_f.bias.data = d["ln_f.bias"]
    drop = nn.Dropout(config.resid_pdrop)
    return wte, wpe, ln_f, drop, config


def _embed(wte, wpe, drop, input_ids):
    """input_ids -> hidden (same as transformer does for layer 0)."""
    position_ids = torch.arange(input_ids.shape[1], device=input_ids.device).unsqueeze(0)
    hidden = wte(input_ids) + wpe(position_ids)
    return drop(hidden)


def _decode(wte, ln_f, hidden):
    """hidden -> logits using ln_f and tied lm_head (wte.weight)."""
    normed = ln_f(hidden)
    logits = torch.nn.functional.linear(normed, wte.weight, None)
    return logits


def _send_and_recv(pub, sub, rid, input_ids, wte, wpe, drop, ln_f, tokenizer):
    t_send = time.time()
    hidden = _embed(wte, wpe, drop, input_ids)
    pub.send_multipart([b"input", _serialize({"rid": rid, "hidden_out": hidden})])
    _, payload = sub.recv_multipart()
    result = _deserialize(payload)
    t_done = time.time()
    rtt = (t_done - t_send) * 1000
    out_hidden = result["hidden_out"]
    logits = _decode(wte, ln_f, out_hidden)
    pred_ids = logits.argmax(dim=-1).squeeze(0).tolist()
    pred_text = tokenizer.decode(pred_ids)
    input_text = tokenizer.decode(input_ids.squeeze(0).tolist())
    print(f"  input tokens : {input_ids.squeeze(0).tolist()}")
    print(f"  input text   : \"{input_text}\"")
    print(f"  output tokens: {pred_ids}")
    print(f"  output text  : \"{pred_text}\"")
    print(f"  rtt={rtt:.1f}ms  hidden_shape={out_hidden.shape}")
    return rtt


def _parse_input(line, tokenizer):
    try:
        ids = [int(t) for t in line.split()]
        return torch.tensor([ids], dtype=torch.long)
    except ValueError:
        pass
    return torch.tensor([tokenizer.encode(line)], dtype=torch.long)


def run_interactive(pub, sub, wte, wpe, ln_f, drop, tokenizer):
    rid = 0
    print("\n" + "=" * 60)
    print("GPT-2 Full 12-Layer CTG - Interactive Feeder (per-role weights)")
    print("=" * 60)
    print('Type text or token IDs. Enter = random prompt. Type "quit" to exit.\n')
    while True:
        try:
            line = input(f"[rid={rid}]> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[feeder] shutting down.")
            break
        if line.lower() in ("quit", "exit", "q"):
            break
        if line:
            input_ids = _parse_input(line, tokenizer)
        else:
            prompts = ["The cat sat on the", "Hello, my name is", "Once upon a time", "The weather today is"]
            text = prompts[rid % len(prompts)]
            input_ids = torch.tensor([tokenizer.encode(text)], dtype=torch.long)
            print(f"  (using: \"{text}\")")
        _send_and_recv(pub, sub, rid, input_ids, wte, wpe, drop, ln_f, tokenizer)
        print()
        rid += 1


def run_auto(pub, sub, wte, wpe, ln_f, drop, tokenizer, count):
    prompts = ["hello", "the cat sat on", "once upon a time", "artificial intelligence is", "the weather today"]
    rtts = []
    for rid in range(count):
        text = prompts[rid % len(prompts)]
        input_ids = torch.tensor([tokenizer.encode(text)], dtype=torch.long)
        print(f"\n--- request {rid+1}/{count} ---")
        rtt = _send_and_recv(pub, sub, rid, input_ids, wte, wpe, drop, ln_f, tokenizer)
        rtts.append(rtt)
    print(f"\n{'='*60}\nAuto mode complete: {count} requests")
    print(f"  avg RTT: {sum(rtts)/len(rtts):.1f}ms  min: {min(rtts):.1f}ms  max: {max(rtts):.1f}ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--auto", type=int, default=0, help="Send N prompts automatically (0=interactive)")
    p.add_argument("--warmup", type=int, default=10, help="Seconds to wait for pipeline")
    args = p.parse_args()

    print("[feeder] Loading tokenizer...")
    tokenizer = GPT2Tokenizer.from_pretrained(os.path.join(WEIGHTS_DIR, "config"))
    print("[feeder] Loading feeder weights (wte, wpe, ln_f)...")
    wte, wpe, ln_f, drop, _ = _load_feeder_weights()

    ctx = zmq.Context()
    pub, sub = _setup_sockets(ctx)
    print(f"[feeder] Waiting {args.warmup}s for pipeline...")
    time.sleep(args.warmup)

    try:
        if args.auto > 0:
            run_auto(pub, sub, wte, wpe, ln_f, drop, tokenizer, args.auto)
        else:
            run_interactive(pub, sub, wte, wpe, ln_f, drop, tokenizer)
    finally:
        pub.close()
        sub.close()
        ctx.term()


if __name__ == "__main__":
    main()
