from __future__ import annotations
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

class _EntryNet(nn.Module):
    def __init__(self, input_dim: int, hidden1: int = 64, hidden2: int = 32):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden1), nn.BatchNorm1d(hidden1), nn.ReLU(), nn.Dropout(0.3),
            nn.Linear(hidden1, hidden2), nn.BatchNorm1d(hidden2), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(hidden2, 2),
        )
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)

class _ExecNet(nn.Module):
    def __init__(self, input_dim: int, num_pairs: int, hidden1: int = 64, hidden2: int = 32):
        super().__init__()
        self.trunk = nn.Sequential(
            nn.Linear(input_dim, hidden1), nn.BatchNorm1d(hidden1), nn.ReLU(), nn.Dropout(0.3),
            nn.Linear(hidden1, hidden2), nn.BatchNorm1d(hidden2), nn.ReLU(), nn.Dropout(0.2),
        )
        self.kelly_head = nn.Sequential(nn.Linear(hidden2, 1), nn.Sigmoid())
        self.pair_head = nn.Linear(hidden2, num_pairs)
    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        h = self.trunk(x)
        return self.kelly_head(h).squeeze(-1), self.pair_head(h)

class MLPEntryEstimator:
    def __init__(self, input_dim: int, hidden1: int = 64, hidden2: int = 32, epochs: int = 30, batch_size: int = 64, lr: float = 0.001):
        self._input_dim = input_dim
        self._hidden1, self._hidden2 = hidden1, hidden2
        self._epochs, self._batch_size, self._lr = epochs, batch_size, lr
        self._net: _EntryNet | None = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._feature_imp: np.ndarray | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        X_t = torch.tensor(X.values, dtype=torch.float32)
        y_t = torch.tensor(y.values, dtype=torch.long)
        w_t = torch.tensor(sample_weight, dtype=torch.float32) if sample_weight is not None else torch.ones(len(y_t))
        self._net = _EntryNet(self._input_dim, self._hidden1, self._hidden2).to(self._device)
        optimizer = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        loss_fn = nn.CrossEntropyLoss(reduction="none")
        loader = DataLoader(TensorDataset(X_t, y_t, w_t), batch_size=self._batch_size, shuffle=True)
        self._net.train()
        for _ in range(self._epochs):
            for xb, yb, wb in loader:
                xb, yb, wb = xb.to(self._device), yb.to(self._device), wb.to(self._device)
                optimizer.zero_grad()
                (loss_fn(self._net(xb), yb) * wb).mean().backward()
                optimizer.step()
        self._compute_feature_importances(X_t)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X_t = torch.tensor(X.values, dtype=torch.float32).to(self._device)
        self._net.eval()
        with torch.no_grad():
            return torch.softmax(self._net(X_t), dim=1).cpu().numpy()

    def _compute_feature_importances(self, X_t: torch.Tensor) -> None:
        X_t = X_t.to(self._device).requires_grad_(True)
        self._net.eval()
        self._net(X_t)[:, 1].sum().backward()
        self._feature_imp = X_t.grad.abs().mean(dim=0).cpu().numpy()

    def feature_importances(self) -> np.ndarray:
        return self._feature_imp if self._feature_imp is not None else np.zeros(self._input_dim)

    def get_params(self) -> dict:
        return {"input_dim": self._input_dim, "hidden1": self._hidden1, "hidden2": self._hidden2, "epochs": self._epochs, "batch_size": self._batch_size, "lr": self._lr}

class MLPExecEstimator:
    def __init__(self, input_dim: int, num_pairs: int, hidden1: int = 64, hidden2: int = 32, epochs: int = 30, batch_size: int = 64, lr: float = 0.001):
        self._input_dim, self._num_pairs = input_dim, num_pairs
        self._hidden1, self._hidden2 = hidden1, hidden2
        self._epochs, self._batch_size, self._lr = epochs, batch_size, lr
        self._net: _ExecNet | None = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._feature_imp: np.ndarray | None = None

    def fit(self, X: pd.DataFrame, y_kelly: pd.Series, y_pair: pd.Series, sample_weight: np.ndarray | None = None) -> None:
        X_t = torch.tensor(X.values, dtype=torch.float32)
        yk_t = torch.tensor(y_kelly.values, dtype=torch.float32)
        yp_t = torch.tensor(y_pair.values, dtype=torch.long)
        w_t = torch.tensor(sample_weight, dtype=torch.float32) if sample_weight is not None else torch.ones(len(X_t))
        self._net = _ExecNet(self._input_dim, self._num_pairs, self._hidden1, self._hidden2).to(self._device)
        optimizer = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        mse_fn, ce_fn = nn.MSELoss(reduction="none"), nn.CrossEntropyLoss(reduction="none")
        loader = DataLoader(TensorDataset(X_t, yk_t, yp_t, w_t), batch_size=self._batch_size, shuffle=True)
        self._net.train()
        for _ in range(self._epochs):
            for xb, kb, pb, wb in loader:
                xb, kb, pb, wb = xb.to(self._device), kb.to(self._device), pb.to(self._device), wb.to(self._device)
                optimizer.zero_grad()
                kelly_pred, pair_logits = self._net(xb)
                ((mse_fn(kelly_pred, kb) * wb).mean() + (ce_fn(pair_logits, pb) * wb).mean()).backward()
                optimizer.step()
        self._compute_feature_importances(X_t)

    def predict(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        X_t = torch.tensor(X.values, dtype=torch.float32).to(self._device)
        self._net.eval()
        with torch.no_grad():
            kelly_pred, pair_logits = self._net(X_t)
        return np.clip(kelly_pred.cpu().numpy(), 0.0, 1.0), pair_logits.argmax(dim=1).cpu().numpy()

    def _compute_feature_importances(self, X_t: torch.Tensor) -> None:
        X_t = X_t.to(self._device).requires_grad_(True)
        self._net.eval()
        self._net(X_t)[0].sum().backward()
        self._feature_imp = X_t.grad.abs().mean(dim=0).cpu().numpy()

    def feature_importances(self) -> np.ndarray:
        return self._feature_imp if self._feature_imp is not None else np.zeros(self._input_dim)

    def get_params(self) -> dict:
        return {"input_dim": self._input_dim, "num_pairs": self._num_pairs, "hidden1": self._hidden1, "hidden2": self._hidden2, "epochs": self._epochs, "batch_size": self._batch_size, "lr": self._lr}
