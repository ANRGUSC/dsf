# Link Scorer - Test Results

## ✅ All Tests Passed

### Unit Tests
- ✅ Penalty computation (5 test cases)
- ✅ Score calculation (edge scoring)
- ✅ Clamp function (5 test cases)
- ✅ Min/Max helpers

### Integration Tests
- ✅ JSON output format
- ✅ CSV output format
- ✅ Top-K filtering
- ✅ Custom parameters (window, capacity, drop-ref)
- ✅ Node filtering
- ✅ Error handling (missing URL, invalid URL, connection errors)

### Scoring Logic Verification
- ✅ Penalty: `clamp(1.0 - drop_bps / drop_ref_bps, 0.1, 1.0)`
- ✅ Residual: `max(0, capacity - usage)`
- ✅ Score: `min(residual_egress(src), residual_ingress(dst)) × penalty(src) × penalty(dst)`

## Sample Output

### JSON Format
```json
{
  "timestamp": "2026-01-27T02:23:09Z",
  "window": "30s",
  "top_k": 10,
  "nodes": {
    "anrg-1": {
      "Node": "anrg-1",
      "EgressBps": 200000000,
      "IngressBps": 180000000,
      "DropBps": 1000000,
      "ResidualEgress": 800000000,
      "ResidualIngress": 820000000,
      "Penalty": 0.9
    }
  },
  "edges": [
    {
      "Src": "anrg-4",
      "Dst": "anrg-2",
      "Score": 880000000
    }
  ]
}
```

### CSV Format
```csv
src,dst,score
anrg-4,anrg-2,880000000.00
anrg-2,anrg-4,850000000.00
```

## Test Coverage
- ✅ Happy path (normal operation)
- ✅ Missing metrics (treated as zero)
- ✅ Invalid inputs (error handling)
- ✅ Edge cases (all nodes, specific nodes, top-K)
- ✅ Different output formats

## Ready for Production
The tool is fully tested and ready to use with your Retina + Prometheus setup.
