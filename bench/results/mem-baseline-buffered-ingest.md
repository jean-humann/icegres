# icegres memory-under-load — 20260718T131633Z

_Baseline: ingest fully buffered the upload (before streaming fix)._

| rows | ingest peak MB | ingest Δ | read-flight peak MB | rf Δ | read-pg peak MB | rpg Δ |
|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 66.7 | +26.3 | 67.8 | +27.5 | 75.4 | +25.3 |
| 500000 | 97.6 | +57.4 | 78.4 | +37.8 | 87.9 | +38.4 |
| 2000000 | 178.4 | +138.2 | 106.8 | +66.3 | 116.9 | +67.5 |
| 4000000 | 283.3 | +243.0 | 138.4 | +98.3 | 149.6 | +100.2 |
