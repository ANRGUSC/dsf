# Sequential Pathload Deployment - Summary

## ✅ Repackaging Complete

The sequential deployment has been repackaged and is ready for testing.

## Structure

```
sequential/
├── README.md                 # Documentation
├── NODE_IPS.md              # Node IP reference
├── measure-3-to-4.yaml      # Pod definitions for 3→4
├── measure-3-to-5.yaml      # Pod definitions for 3→5
├── measure-4-to-5.yaml      # Pod definitions for 4→5
├── run-all.sh               # Script to run all measurements sequentially
└── collect-results.sh       # Script to collect and summarize results
```

## Key Features

1. **Three Measurement Pairs**:
   - anrg-3 → anrg-4 (192.168.1.164 → 192.168.1.156)
   - anrg-3 → anrg-5 (192.168.1.164 → 192.168.1.154)
   - anrg-4 → anrg-5 (192.168.1.156 → 192.168.1.154)

2. **Sequential Execution**: Measurements run one at a time to avoid interference

3. **Automated Scripts**:
   - `run-all.sh`: Orchestrates all measurements sequentially
   - `collect-results.sh`: Collects and summarizes results

4. **Labeled Pods**: All pods have labels for easy filtering:
   - `measurement: "3-to-4"`, `"3-to-5"`, `"4-to-5"`
   - `role: sender` or `role: receiver`

## Recommendations

### ✅ Advantages
- No code modifications needed
- No network interference
- Accurate results
- Simple implementation
- Easy to debug

### ⏱️ Trade-offs
- Takes ~33 seconds total (vs ~11 seconds for concurrent)
- Each measurement waits for previous to complete

## Next Steps

1. Test the sequential deployment:
   ```bash
   cd sequential/
   ./run-all.sh
   ```

2. Collect results:
   ```bash
   ./collect-results.sh
   ```

3. Compare with concurrent approach (once implemented)

## Notes

- All pods use `hostNetwork: true` for direct network access
- Pods are automatically cleaned up between measurements
- Results are extracted from receiver pod logs
