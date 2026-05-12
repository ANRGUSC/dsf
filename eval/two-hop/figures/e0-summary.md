# E0 Results Summary

Stats are taken from the warm window (runs 5+, when ≥5 reps exist).

## E2E mean / std / p95 by cell

| Coloc | Payload | System | n | mean (s) | std (s) | p95 (s) |
|---|---|---|---:|---:|---:|---:|
| same | 1MB | DSF | 0 | – | – | – |
| same | 1MB | MinIO (baseline) | 0 | – | – | – |
| same | 10MB | DSF | 2 | 9.863 | 0.201 | 9.662 |
| same | 10MB | MinIO (baseline) | 2 | 7.060 | 0.002 | 7.058 |
| same | 100MB | DSF | 0 | – | – | – |
| same | 100MB | MinIO (baseline) | 0 | – | – | – |
| same | 500MB | DSF | 0 | – | – | – |
| same | 500MB | MinIO (baseline) | 0 | – | – | – |
| cross | 1MB | DSF | 0 | – | – | – |
| cross | 1MB | MinIO (baseline) | 0 | – | – | – |
| cross | 10MB | DSF | 0 | – | – | – |
| cross | 10MB | MinIO (baseline) | 0 | – | – | – |
| cross | 100MB | DSF | 0 | – | – | – |
| cross | 100MB | MinIO (baseline) | 0 | – | – | – |
| cross | 500MB | DSF | 0 | – | – | – |
| cross | 500MB | MinIO (baseline) | 0 | – | – | – |

## DSF vs MinIO ratios (warm mean)

| Coloc | Payload | MinIO (s) | DSF (s) | Ratio (MinIO/DSF) |
|---|---|---:|---:|---:|
| same | 1MB | – | – | – |
| same | 10MB | 7.060 | 9.863 | 0.72× |
| same | 100MB | – | – | – |
| same | 500MB | – | – | – |
| cross | 1MB | – | – | – |
| cross | 10MB | – | – | – |
| cross | 100MB | – | – | – |
| cross | 500MB | – | – | – |

## Pass/fail

⚠️ No same-node ≥100MB results yet — verdict deferred.
