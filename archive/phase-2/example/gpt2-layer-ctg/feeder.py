#!/usr/bin/env python3
"""
Interactive feeder for the GPT-2 CTG single-layer DAG.

Publishes input token IDs to qkv, subscribes to mlp-merge for the result.
Supports text input ("hello world") and shows predicted output tokens.

Usage (from inside the feeder pod):
  python feeder.py               # interactive mode
  python feeder.py --auto 5      # send 5 random text prompts automatically
"""

import argparse
import io
import os
import time

import torch
import zmq
from transformers import GPT2LMHeadModel, GPT2Tokenizer

ZMQ_PORT = 5555
LINGER_MS = 1000
MODEL_CACHE = "/app/model_cache"

GRAPH_NAME = os.environ.get("ZMQ_GRAPH_NAME", "gpt2-ctg")
NAMESPACE = os.environ.get("ZMQ_NAMESPACE", "default")


def _serialize(tensors: dict) -> bytes:
    buf = io.BytesIO()
    torch.save(tensors, buf)
    return buf.getvalue()


def _deserialize(data: bytes) -> dict:
    return torch.load(io.BytesIO(data), weights_only=False)


def _svc_addr(task: str) -> str:
    return (f"tcp://{GRAPH_NAME}-{task}-service"
            f".{NAMESPACE}.svc.cluster.local:{ZMQ_PORT}")


def _setup_sockets(ctx):
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.LINGER, LINGER_MS)
    pub.bind(f"tcp://*:{ZMQ_PORT}")
    print(f"[feeder] PUB bound on port {ZMQ_PORT}")

    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, LINGER_MS)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    mlp_merge_addr = _svc_addr("mlp-merge")
    sub.connect(mlp_merge_addr)
    print(f"[feeder] SUB connected to {mlp_merge_addr}")

    return pub, sub


def _load_head():
    """Load pretrained GPT-2 to get ln_f + lm_head for decoding."""
    model = GPT2LMHeadModel.from_pretrained("gpt2", cache_dir=MODEL_CACHE)
    model.eval()
    return model


def _hidden_to_tokens(model, tokenizer, hidden):
    """Project hidden states -> logits -> greedy tokens -> text."""
    with torch.no_grad():
        normed = model.transformer.ln_f(hidden)
        logits = model.lm_head(normed)
    pred_ids = logits.argmax(dim=-1).squeeze(0).tolist()
    pred_text = tokenizer.decode(pred_ids)
    return pred_ids, pred_text


def _send_and_recv(pub, sub, rid, input_ids, model, tokenizer):
    t_send = time.time()
    pub.send_multipart([b"input", _serialize({"rid": rid, "input_ids": input_ids})])

    _, payload = sub.recv_multipart()
    result = _deserialize(payload)
    t_done = time.time()

    rtt = (t_done - t_send) * 1000
    hidden = result["hidden_out"]

    pred_ids, pred_text = _hidden_to_tokens(model, tokenizer, hidden)
    input_text = tokenizer.decode(input_ids.squeeze(0).tolist())

    print(f"  input tokens : {input_ids.squeeze(0).tolist()}")
    print(f"  input text   : \"{input_text}\"")
    print(f"  output tokens: {pred_ids}")
    print(f"  output text  : \"{pred_text}\"")
    print(f"  rtt={rtt:.1f}ms  hidden_shape={hidden.shape}")
    return rtt


def _parse_input(line, tokenizer):
    """Parse user input: text string or space-separated token IDs."""
    try:
        ids = [int(t) for t in line.split()]
        return torch.tensor([ids], dtype=torch.long)
    except ValueError:
        pass
    ids = tokenizer.encode(line)
    return torch.tensor([ids], dtype=torch.long)


def run_interactive(pub, sub, model, tokenizer):
    rid = 0
    print("\n" + "=" * 60)
    print("GPT-2 Single-Layer CTG - Interactive Feeder (pretrained)")
    print("=" * 60)
    print('Type text (e.g. "hello world") or token IDs (e.g. "15496 995").')
    print("Press Enter for a random prompt. Type 'quit' to exit.\n")

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
            prompts = ["The cat sat on the", "Hello, my name is",
                       "Once upon a time", "The weather today is"]
            text = prompts[rid % len(prompts)]
            input_ids = torch.tensor([tokenizer.encode(text)], dtype=torch.long)
            print(f"  (using: \"{text}\")")

        _send_and_recv(pub, sub, rid, input_ids, model, tokenizer)
        print()
        rid += 1


def run_auto(pub, sub, model, tokenizer, count):
    prompts = ["hello", "the cat sat on", "once upon a time",
               "artificial intelligence is", "the weather today"]
    rtts = []
    for rid in range(count):
        text = prompts[rid % len(prompts)]
        input_ids = torch.tensor([tokenizer.encode(text)], dtype=torch.long)
        print(f"\n--- request {rid+1}/{count} ---")
        rtt = _send_and_recv(pub, sub, rid, input_ids, model, tokenizer)
        rtts.append(rtt)

    print(f"\n{'='*60}")
    print(f"Auto mode complete: {count} requests")
    print(f"  avg RTT: {sum(rtts)/len(rtts):.1f}ms")
    print(f"  min RTT: {min(rtts):.1f}ms")
    print(f"  max RTT: {max(rtts):.1f}ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--auto", type=int, default=0,
                   help="Send N random text prompts automatically (0 = interactive)")
    p.add_argument("--warmup", type=int, default=5,
                   help="Seconds to wait for ZeroMQ connections (default: 5)")
    args = p.parse_args()

    print("[feeder] Loading tokenizer...")
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2", cache_dir=MODEL_CACHE)

    print("[feeder] Loading pretrained GPT-2 head (ln_f + lm_head)...")
    model = _load_head()

    ctx = zmq.Context()
    pub, sub = _setup_sockets(ctx)

    print(f"[feeder] Waiting {args.warmup}s for pipeline nodes to connect...")
    time.sleep(args.warmup)

    try:
        if args.auto > 0:
            run_auto(pub, sub, model, tokenizer, args.auto)
        else:
            run_interactive(pub, sub, model, tokenizer)
    finally:
        pub.close()
        sub.close()
        ctx.term()


if __name__ == "__main__":
    main()
