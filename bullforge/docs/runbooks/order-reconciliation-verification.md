# Order Reconciliation — Live Verification

## Deploy
1. `cd bullforge && bin/up execution_service` (picks up migrations 010 and 011).
2. Tail logs: `docker compose logs -f execution_service | grep -i reconcil`.

## Confirm the six known-stuck orders reconcile
Within one `EXECUTION_RECONCILER_POLL_INTERVAL_S` cycle (default 60s) after boot:

```sql
SELECT alpaca_order_id, status, reconcile_source, last_reconciled_at
FROM orders
WHERE status NOT IN ('filled','cancelled','rejected','expired')
ORDER BY submitted_at DESC;
```

Expected: the previously-stuck rows now show `status='cancelled'`,
`reconcile_source='startup'` or `'poll'`, and a recent
`last_reconciled_at`.

## Watch counters
`redis-cli SUBSCRIBE monitoring:reconciler` — expect
`reconciler_events_total` on every status transition and
`reconciler_stream_reconnects_total=0` during steady state.

## Rollback escape hatch
Set `EXECUTION_RECONCILER_STREAM_ENABLED=false` if the Alpaca stream
is flapping; the polling sweeper alone still closes the gap.
