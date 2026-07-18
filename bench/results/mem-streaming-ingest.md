# icegres memory-under-load — 20260718T134001Z

_After: streaming ingest (bounded by rolling-writer file size)._

| rows | ingest peak MB | ingest Δ | read-flight peak MB | rf Δ | read-pg peak MB | rpg Δ |
|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 57.9 | +18.4 | 67.9 | +27.5 | 74.9 | +25.0 |
| 500000 | 70.5 | +30.2 | 70.7 | +30.7 | 75.8 | +27.5 |
| 2000000 | 81.5 | +41.6 | 73.1 | +32.8 | 80.9 | +31.8 |
| 4000000 | 100.2 | +60.3 | 76.5 | +36.7 | 85.1 | +35.1 |
