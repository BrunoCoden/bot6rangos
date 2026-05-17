"""
Configuración compartida para rutas de backtests.
"""
from __future__ import annotations

import os
from pathlib import Path


OUTPUT_PRESETS: dict[str, dict[str, Path]] = {
    "tr": {
        "trades": Path(os.getenv("STRAT_BACKTEST_TRADES_PATH", "backtest/backtestTR/trades.csv")),
        "plot": Path(os.getenv("STRAT_BACKTEST_PLOT_PATH", "backtest/backtestTR/plot.png")),
        "dashboard": Path(os.getenv("STRAT_BACKTEST_DASHBOARD_PATH", "backtest/backtestTR/dashboard.html")),
    },
    "historico": {
        "trades": Path(os.getenv("STRAT_HIST_BACKTEST_TRADES_PATH", "backtest/backtestHistorico/trades.csv")),
        "plot": Path(os.getenv("STRAT_HIST_BACKTEST_PLOT_PATH", "backtest/backtestHistorico/plot.png")),
        "dashboard": Path(os.getenv("STRAT_HIST_BACKTEST_DASHBOARD_PATH", "backtest/backtestHistorico/dashboard.html")),
    },
}

DEFAULT_PROFILE = os.getenv("BACKTEST_PROFILE", "tr").lower()


def resolve_profile(profile: str | None) -> str:
    candidate = (profile or DEFAULT_PROFILE).lower()
    return candidate if candidate in OUTPUT_PRESETS else "tr"
