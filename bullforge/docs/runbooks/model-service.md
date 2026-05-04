# Model Service — Operator Runbook

## Cleaning up partial / corrupt artifact pickles

A worker process that dies mid-write during the nightly retrain (e.g., the
2026-05-04 OpenMP segfault) can leave a truncated `.joblib` or `.pt` file
in the artifacts volume. After `fix/model-service-retrain-resilience` (commits
`2214b98` + `e544392`), `InferenceEngine.load_models` and `load_meta_models`
log + skip these on load rather than nullifying the in-memory model dict —
but the bad files still sit on disk and pollute every subsequent load with
an ERROR-level log line each.

To clean them up:

```bash
# Inspect first — list any artifact under 1 KiB written in the last 24h.
docker compose exec model_service \
  find /app/models/artifacts -mmin -1440 -size -1k \
       -type f \( -name '*.joblib' -o -name '*.pt' \)

# If the listing matches what you expect, delete them.
docker compose exec model_service \
  find /app/models/artifacts -mmin -1440 -size -1k \
       -type f \( -name '*.joblib' -o -name '*.pt' \) -delete
```

Why these filters:

- `-mmin -1440` scopes to the last 24 hours so older legitimate artifacts are
  untouched. Widen the window only if the bad write happened earlier.
- `-size -1k` is a heuristic — a real model artifact is at minimum tens of
  KiB. Verify the listing first; an empty `find` result means there's
  nothing to clean.
- The extension filter excludes the `backtest/` subtree's per-fold layout —
  it's already namespaced under `backtest/{run_id}/fold_{idx}/` and is
  cleaned by backtest run deletion, not this command.

After deletion the next inference reload (manual `retrain_now` or the next
nightly cycle) silently stops emitting the "Failed to load artifact" ERROR
line for that file because the corresponding `model_registry` row will
either be replaced by the next training cycle or be flagged as missing
(falls into the existing `FileNotFoundError` warning branch).

If the partial artifact has a corresponding `model_registry` row that the
next retrain won't replace (e.g., the bad row's `(symbol, timeframe,
horizon, model_type)` slot was retired), set `is_active=false` on it
manually so it doesn't keep getting picked up by `query_all_active_models`.
