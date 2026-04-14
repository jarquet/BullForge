from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

logger = logging.getLogger(__name__)

_LABEL_TO_CLASS = {-1: 0, 0: 1, 1: 2}
_CLASS_TO_LABEL = {v: k for k, v in _LABEL_TO_CLASS.items()}


class _LSTMNet(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_classes: int = 3):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        return self.fc(last)


def _make_sequences(
    X: np.ndarray, y: Optional[np.ndarray], seq_len: int,
) -> tuple[np.ndarray, Optional[np.ndarray]]:
    """Turn flat (N, F) rows into (N-seq_len+1, seq_len, F) sequences aligned with y[seq_len-1:]."""
    n = len(X)
    if n < seq_len:
        raise ValueError(f"Need at least {seq_len} rows, got {n}")
    seqs = np.stack([X[i : i + seq_len] for i in range(n - seq_len + 1)], axis=0)
    y_aligned = None if y is None else y[seq_len - 1 :]
    return seqs, y_aligned


class LSTMClassifier:
    """sklearn-compatible PyTorch LSTM classifier for {-1, 0, 1} direction labels."""

    def __init__(
        self,
        hidden_size: int = 64,
        sequence_length: int = 30,
        num_classes: int = 3,
        lr: float = 0.001,
        epochs: int = 50,
        batch_size: int = 64,
        early_stopping_patience: int = 5,
        val_ratio: float = 0.2,
        random_state: int = 42,
    ):
        self.hidden_size = hidden_size
        self.sequence_length = sequence_length
        self.num_classes = num_classes
        self.lr = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.early_stopping_patience = early_stopping_patience
        self.val_ratio = val_ratio
        self.random_state = random_state
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._net: Optional[_LSTMNet] = None
        self._feature_columns: Optional[list[str]] = None
        self.epochs_trained_: int = 0

    def _to_classes(self, y: pd.Series) -> np.ndarray:
        return np.array([_LABEL_TO_CLASS[int(v)] for v in y], dtype=np.int64)

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "LSTMClassifier":
        torch.manual_seed(self.random_state)
        self._feature_columns = list(X.columns)
        X_arr = X.to_numpy(dtype=np.float32)
        y_arr = self._to_classes(y)

        X_seq, y_seq = _make_sequences(X_arr, y_arr, self.sequence_length)

        split = int(len(X_seq) * (1 - self.val_ratio))
        X_tr, X_val = X_seq[:split], X_seq[split:]
        y_tr, y_val = y_seq[:split], y_seq[split:]

        train_ds = TensorDataset(
            torch.from_numpy(X_tr), torch.from_numpy(y_tr),
        )
        train_dl = DataLoader(train_ds, batch_size=self.batch_size, shuffle=False)
        X_val_t = torch.from_numpy(X_val).to(self.device)
        y_val_t = torch.from_numpy(y_val).to(self.device)

        input_size = X_arr.shape[1]
        self._net = _LSTMNet(input_size, self.hidden_size, self.num_classes).to(self.device)
        optim = torch.optim.Adam(self._net.parameters(), lr=self.lr)
        criterion = nn.CrossEntropyLoss()

        best_val_loss = float("inf")
        best_state = None
        patience_left = self.early_stopping_patience

        for epoch in range(self.epochs):
            self._net.train()
            for xb, yb in train_dl:
                xb, yb = xb.to(self.device), yb.to(self.device)
                optim.zero_grad()
                logits = self._net(xb)
                loss = criterion(logits, yb)
                loss.backward()
                optim.step()

            self._net.eval()
            with torch.no_grad():
                val_logits = self._net(X_val_t)
                val_loss = criterion(val_logits, y_val_t).item()

            self.epochs_trained_ = epoch + 1
            if val_loss < best_val_loss - 1e-5:
                best_val_loss = val_loss
                best_state = {k: v.clone() for k, v in self._net.state_dict().items()}
                patience_left = self.early_stopping_patience
            else:
                patience_left -= 1
                if patience_left <= 0:
                    logger.info("LSTM early stopping at epoch %d (val_loss=%.4f)",
                                epoch + 1, best_val_loss)
                    break

        if best_state is not None:
            self._net.load_state_dict(best_state)
        return self

    def _predict_logits(self, X: pd.DataFrame) -> np.ndarray:
        assert self._net is not None, "Must call fit() before predict"
        X_arr = X.to_numpy(dtype=np.float32)
        X_seq, _ = _make_sequences(X_arr, None, self.sequence_length)
        self._net.eval()
        with torch.no_grad():
            logits = self._net(torch.from_numpy(X_seq).to(self.device))
        logits_np = logits.cpu().numpy()
        pad = np.repeat(logits_np[:1], self.sequence_length - 1, axis=0)
        return np.concatenate([pad, logits_np], axis=0)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        logits = self._predict_logits(X)
        cls = logits.argmax(axis=1)
        return np.array([_CLASS_TO_LABEL[int(c)] for c in cls])

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        logits = self._predict_logits(X)
        t = torch.from_numpy(logits)
        return torch.softmax(t, dim=1).numpy()
