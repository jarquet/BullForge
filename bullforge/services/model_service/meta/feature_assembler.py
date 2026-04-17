from __future__ import annotations
from datetime import datetime

class MetaFeatureAssembler:
    """Builds the L2 meta-feature vector from base predictions + context."""
    _MARKET_OPEN_UTC_MINUTES = 13 * 60 + 30  # 9:30 ET = 13:30 UTC
    _MARKET_CLOSE_UTC_MINUTES = 20 * 60       # 16:00 ET = 20:00 UTC

    def __init__(self, model_ids: list[int]):
        self._model_ids = sorted(model_ids)
        self._model_idx = {mid: i for i, mid in enumerate(self._model_ids)}

    def assemble(self, predictions: list[dict], rolling_stats: dict[int, dict],
                 regime: dict[str, float], symbol_state: dict[str, float],
                 timestamp: datetime) -> dict[str, float]:
        vec: dict[str, float] = {}
        pred_by_id = {p["model_id"]: p for p in predictions}
        for mid in self._model_ids:
            idx = self._model_idx[mid]
            prefix = f"base_{idx + 1}"
            p = pred_by_id.get(mid)
            if p is not None:
                vec[f"{prefix}_dir"] = float(p["direction"])
                vec[f"{prefix}_conf"] = float(p["confidence"])
            else:
                vec[f"{prefix}_dir"] = 0.0
                vec[f"{prefix}_conf"] = 0.0
            stats = rolling_stats.get(mid, {})
            vec[f"{prefix}_rolling_acc"] = float(stats.get("accuracy", 0.0))
            vec[f"{prefix}_cal_err"] = float(stats.get("calibration_error", 0.0))
            vec[f"{prefix}_disabled"] = 1.0 if stats.get("is_disabled", False) else 0.0

        vec["regime_spy_trend_1m"] = regime.get("spy_trend_1m", 0.0)
        vec["regime_spy_trend_1h"] = regime.get("spy_trend_1h", 0.0)
        vec["regime_spy_trend_1d"] = regime.get("spy_trend_1d", 0.0)
        vec["regime_spy_momentum"] = regime.get("spy_momentum", 0.0)
        vec["regime_vol"] = regime.get("vol_regime", 0.0)

        minutes_utc = timestamp.hour * 60 + timestamp.minute
        vec["time_bucket"] = float(minutes_utc // 15)
        vec["minutes_since_open"] = float(max(0, minutes_utc - self._MARKET_OPEN_UTC_MINUTES))
        vec["minutes_to_close"] = float(max(0, self._MARKET_CLOSE_UTC_MINUTES - minutes_utc))
        vec["weekday"] = float(timestamp.weekday())

        vec["sym_realized_vol"] = symbol_state.get("realized_vol", 0.0)
        vec["sym_atr_price_ratio"] = symbol_state.get("atr_price_ratio", 0.0)
        vec["sym_dist_sma_50"] = symbol_state.get("dist_sma_50", 0.0)
        vec["sym_dist_sma_200bar"] = symbol_state.get("dist_sma_200bar", 0.0)
        vec["sym_dist_sma_200day"] = symbol_state.get("dist_sma_200day", 0.0)
        return vec

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for mid in self._model_ids:
            idx = self._model_idx[mid]
            prefix = f"base_{idx + 1}"
            names.extend([f"{prefix}_dir", f"{prefix}_conf", f"{prefix}_rolling_acc", f"{prefix}_cal_err", f"{prefix}_disabled"])
        names.extend(["regime_spy_trend_1m", "regime_spy_trend_1h", "regime_spy_trend_1d", "regime_spy_momentum", "regime_vol"])
        names.extend(["time_bucket", "minutes_since_open", "minutes_to_close", "weekday"])
        names.extend(["sym_realized_vol", "sym_atr_price_ratio", "sym_dist_sma_50", "sym_dist_sma_200bar", "sym_dist_sma_200day"])
        return names
