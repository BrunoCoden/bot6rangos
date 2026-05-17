# build_dashboard.py
import argparse
import html
import os
import sys
import webbrowser
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

try:
    from velas import SYMBOL_DISPLAY, STREAM_INTERVAL
except ImportError:
    CURRENT_DIR = Path(__file__).resolve().parent
    PARENT_DIR = CURRENT_DIR.parent
    if str(PARENT_DIR) not in sys.path:
        sys.path.append(str(PARENT_DIR))
    from velas import SYMBOL_DISPLAY, STREAM_INTERVAL

try:
    from .config import OUTPUT_PRESETS, resolve_profile
except ImportError:  # ejecución directa
    CURRENT_DIR = Path(__file__).resolve().parent
    if str(CURRENT_DIR) not in sys.path:
        sys.path.append(str(CURRENT_DIR))
    if str(CURRENT_DIR.parent) not in sys.path:
        sys.path.append(str(CURRENT_DIR.parent))
    from config import OUTPUT_PRESETS, resolve_profile

DEFAULT_PRICE_PATH = Path(os.getenv("ALERTS_TABLE_CSV_PATH", "alerts_stream.csv"))

LOGO_SVG = """
<svg width=\"72\" height=\"72\" viewBox=\"0 0 120 120\" xmlns=\"http://www.w3.org/2000/svg\">
  <rect x=\"0\" y=\"0\" width=\"120\" height=\"120\" rx=\"18\" fill=\"#111827\" stroke=\"#2563eb\" stroke-width=\"6\"/>
  <path d=\"M15 60 C30 40, 55 20, 80 45 S115 100, 105 105\" stroke=\"#22d3ee\" stroke-width=\"6\" fill=\"none\"/>
  <path d=\"M15 80 C40 65, 65 50, 90 70\" stroke=\"#a855f7\" stroke-width=\"6\" fill=\"none\" opacity=\"0.8\"/>
  <circle cx=\"78\" cy=\"46\" r=\"8\" fill=\"#facc15\" stroke=\"#facc15\"/>
</svg>
"""


def load_trades(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de trades: {path}")
    df = pd.read_csv(path)
    if "EntryTime" in df.columns:
        df["EntryTime"] = pd.to_datetime(df["EntryTime"])
    if "ExitTime" in df.columns:
        df["ExitTime"] = pd.to_datetime(df["ExitTime"])
    if "OrderTime" in df.columns:
        df["OrderTime"] = pd.to_datetime(df["OrderTime"])
    else:
        df["OrderTime"] = df.get("EntryTime")
    return df


def load_price(path: Path | None) -> pd.DataFrame | None:
    if not path:
        return None
    if not path.exists():
        print(f"[DASHBOARD][WARN] Archivo de precios no encontrado: {path}")
        return None
    df = pd.read_csv(path, parse_dates=["Timestamp"])
    df.set_index("Timestamp", inplace=True)
    return df


def summarize_trades(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"Total trades": 0}
    wins = (df["Outcome"] == "win").sum()
    losses = (df["Outcome"] == "loss").sum()
    total = len(df)
    pnl_pct_sum = df["PnLPct"].sum() * 100
    pnl_pct_avg = df["PnLPct"].mean() * 100
    winrate = wins / total * 100 if total else 0
    cum = df["PnLPct"].fillna(0).cumsum()
    max_drawdown = cum.min() * 100
    total_fees = df.get("Fees", pd.Series(dtype=float)).sum()
    return {
        "Total trades": total,
        "Wins": wins,
        "Losses": losses,
        "Win rate %": f"{winrate:.2f}",
        "Total PnL %": f"{pnl_pct_sum:.2f}",
        "Avg PnL %": f"{pnl_pct_avg:.2f}",
        "Max Drawdown %": f"{max_drawdown:.2f}",
        "Total Fees": f"{total_fees:.2f}",
    }


def build_figure(trades: pd.DataFrame, price_df: pd.DataFrame | None):
    if trades.empty:
        raise ValueError("No hay trades para mostrar.")

    rows = 3 if price_df is not None else 2
    specs = [[{"type": "xy"}] for _ in range(rows)]
    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        specs=specs,
        row_heights=[0.5, 0.3, 0.2] if rows == 3 else [0.6, 0.4],
    )

    row_idx = 1
    if price_df is not None:
        fig.add_trace(
            go.Scatter(
                x=price_df.index,
                y=price_df["Close"],
                name="Close",
                line=dict(color="#222", width=1.2),
                hovertemplate="%{x}<br>Close: %{y:.2f}<extra></extra>",
            ),
            row=row_idx,
            col=1,
        )

        long_entries = trades[trades["Direction"] == "long"]
        short_entries = trades[trades["Direction"] == "short"]

        if not long_entries.empty:
            fig.add_trace(
                go.Scatter(
                    x=long_entries["EntryTime"],
                    y=long_entries["EntryPrice"],
                    mode="markers",
                    name="Long Entry",
                    marker=dict(symbol="triangle-up", color="#16a34a", size=9),
                    hovertemplate="Long Entry<br>%{x}<br>%{y:.2f}<extra></extra>",
                ),
                row=row_idx,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=long_entries["ExitTime"],
                    y=long_entries["ExitPrice"],
                    mode="markers",
                    name="Long Exit",
                    marker=dict(symbol="x", color="#16a34a", size=9),
                    hovertemplate="Long Exit<br>%{x}<br>%{y:.2f}<extra></extra>",
                ),
                row=row_idx,
                col=1,
            )

        if not short_entries.empty:
            fig.add_trace(
                go.Scatter(
                    x=short_entries["EntryTime"],
                    y=short_entries["EntryPrice"],
                    mode="markers",
                    name="Short Entry",
                    marker=dict(symbol="triangle-down", color="#dc2626", size=9),
                    hovertemplate="Short Entry<br>%{x}<br>%{y:.2f}<extra></extra>",
                ),
                row=row_idx,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=short_entries["ExitTime"],
                    y=short_entries["ExitPrice"],
                    mode="markers",
                    name="Short Exit",
                    marker=dict(symbol="x", color="#dc2626", size=9),
                    hovertemplate="Short Exit<br>%{x}<br>%{y:.2f}<extra></extra>",
                ),
                row=row_idx,
                col=1,
            )

        row_idx += 1

    cum_pct = trades["PnLPct"].fillna(0).cumsum() * 100
    fig.add_trace(
        go.Scatter(
            x=trades["ExitTime"],
            y=cum_pct,
            mode="lines+markers",
            name="PnL acumulado %",
            line=dict(color="#2563eb", width=2),
            marker=dict(size=6),
            hovertemplate="%{x}<br>%{y:.2f}%<extra></extra>",
        ),
        row=row_idx,
        col=1,
    )
    fig.update_yaxes(title_text="PnL %", row=row_idx, col=1)
    row_idx += 1

    fig.add_trace(
        go.Histogram(
            x=trades["PnLPct"] * 100,
            nbinsx=20,
            marker=dict(color="#737373"),
            name="Distribución PnL %",
            hovertemplate="%{x:.2f}%<extra></extra>",
        ),
        row=row_idx,
        col=1,
    )
    fig.update_yaxes(title_text="Frecuencia", row=row_idx, col=1)
    fig.update_xaxes(title_text="PnL (%)", row=row_idx, col=1)

    fig.update_layout(
        height=780,
        template="plotly_white",
        title="Dashboard Estrategia Bollinger",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def build_summary_html(summary: dict) -> str:
    rows = "".join(f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in summary.items())
    return f"""
    <section class="summary">
        <h2>Resumen</h2>
        <table>
            {rows}
        </table>
    </section>
    """


def _fmt_two(value, *, blank: str = "") -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return blank
    if pd.isna(num):
        return blank
    return f"{num:.2f}"


def _fmt_pct(value, *, blank: str = "") -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return blank
    if pd.isna(num):
        return blank
    return f"{num * 100:.5f}%"


def _fmt_timestamp(value, fmt: str) -> str:
    if value is None:
        return ""
    try:
        ts = pd.Timestamp(value)
    except Exception:
        text = str(value)
        return "" if not text or text.lower() in {"nat", "nan"} else text
    if pd.isna(ts):
        return ""
    return ts.strftime(fmt)


def _safe_text(value, *, blank: str = "") -> str:
    if value is None:
        return blank
    if isinstance(value, float) and pd.isna(value):
        return blank
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat"}:
        return blank
    return text


def _normalize_value(value) -> str:
    text = _safe_text(value, blank="")
    return text.lower()


def _data_attr_name(key: str) -> str:
    parts: list[str] = []
    for ch in key:
        if ch.isupper():
            parts.append("-")
            parts.append(ch.lower())
        else:
            parts.append(ch)
    return "data-" + "".join(parts)


def build_trades_table(trades: pd.DataFrame) -> str:
    columns = [
        ("EntryTime", "Entrada"),
        ("OrderTime", "Orden Banda"),
        ("ExitTime", "Salida"),
        ("Direction", "Dirección"),
        ("EntryReason", "Motivo Entrada"),
        ("ExitReason", "Motivo Salida"),
        ("EntryPrice", "Precio Entrada"),
        ("ExitPrice", "Precio Salida"),
        ("Outcome", "Resultado"),
        ("PnLAbs", "PnL"),
        ("PnLPct", "PnL %"),
        ("Fees", "Fees"),
    ]

    header_cells = "".join(f"<th>{label}</th>" for _, label in columns)
    rows_html: list[str] = []

    for _, row in trades.iterrows():
        attrs = {
            "direction": _normalize_value(row.get("Direction")),
            "entryReason": _normalize_value(row.get("EntryReason")),
            "exitReason": _normalize_value(row.get("ExitReason")),
            "outcome": _normalize_value(row.get("Outcome")),
        }
        attr_parts = [
            f'{_data_attr_name(key)}="{html.escape(value)}"'
            for key, value in attrs.items()
            if value
        ]
        attr_str = f" {' '.join(attr_parts)}" if attr_parts else ""

        cells: list[str] = []
        direction_raw = _safe_text(row.get("Direction"), blank="")
        direction_norm = direction_raw.lower() if direction_raw else ""
        outcome_raw = _safe_text(row.get("Outcome"), blank="")
        outcome_norm = outcome_raw.lower() if outcome_raw else ""

        for key, _ in columns:
            if key in {"EntryTime", "OrderTime", "ExitTime"}:
                text = _fmt_timestamp(row.get(key), "%Y-%m-%d %H:%M:%S")
                cells.append(f"<td>{html.escape(text)}</td>")
            elif key in {"EntryPrice", "ExitPrice", "PnLAbs", "Fees"}:
                text = _fmt_two(row.get(key), blank="")
                cells.append(f"<td>{html.escape(text)}</td>")
            elif key == "PnLPct":
                text = _fmt_pct(row.get(key), blank="")
                cells.append(f"<td>{html.escape(text)}</td>")
            elif key == "Direction":
                if direction_raw:
                    cells.append(
                        f"<td class='dir {direction_norm}'>{html.escape(direction_raw.upper())}</td>"
                    )
                else:
                    cells.append("<td></td>")
            elif key == "Outcome":
                if outcome_raw:
                    cells.append(
                        f"<td class='result {outcome_norm}'>{html.escape(outcome_raw.upper())}</td>"
                    )
                else:
                    cells.append("<td></td>")
            else:
                text = _safe_text(row.get(key), blank="")
                cells.append(f"<td>{html.escape(text)}</td>")

        rows_html.append(f"<tr{attr_str}>{''.join(cells)}</tr>")

    body_rows = "".join(rows_html)
    return f"""
    <section class="trades">
        <h2>Todos los trades</h2>
        <table class="trades-table filterable">
            <thead><tr>{header_cells}</tr></thead>
            <tbody>{body_rows}</tbody>
        </table>
    </section>
    """


def build_operations_table(trades: pd.DataFrame, limit: int = 15) -> str:
    columns = [
        ("EntryTime", "Entrada"),
        ("OrderTime", "Orden Banda"),
        ("Direction", "Dirección"),
        ("EntryReason", "Motivo Entrada"),
        ("ExitReason", "Motivo Salida"),
        ("ExitTime", "Salida"),
        ("EntryPrice", "Precio Entrada"),
        ("ExitPrice", "Precio Salida"),
        ("Outcome", "Resultado"),
        ("PnLAbs", "PnL"),
        ("PnLPct", "PnL %"),
        ("Fees", "Fees"),
    ]

    subset = trades.tail(limit)
    header_cells = "".join(f"<th>{label}</th>" for _, label in columns)
    rows_html: list[str] = []

    for _, row in subset.iterrows():
        attrs = {
            "direction": _normalize_value(row.get("Direction")),
            "entryReason": _normalize_value(row.get("EntryReason")),
            "exitReason": _normalize_value(row.get("ExitReason")),
            "outcome": _normalize_value(row.get("Outcome")),
        }
        attr_parts = [
            f'{_data_attr_name(key)}="{html.escape(value)}"'
            for key, value in attrs.items()
            if value
        ]
        attr_str = f" {' '.join(attr_parts)}" if attr_parts else ""

        direction_raw = _safe_text(row.get("Direction"), blank="")
        direction_norm = direction_raw.lower() if direction_raw else ""
        outcome_raw = _safe_text(row.get("Outcome"), blank="")
        outcome_norm = outcome_raw.lower() if outcome_raw else ""

        cells: list[str] = []
        for key, _ in columns:
            if key in {"EntryTime", "OrderTime", "ExitTime"}:
                text = _fmt_timestamp(row.get(key), "%d-%m %H:%M")
                cells.append(f"<td>{html.escape(text or '--')}</td>")
            elif key in {"EntryPrice", "ExitPrice", "PnLAbs", "Fees"}:
                text = _fmt_two(row.get(key), blank="--")
                cells.append(f"<td>{html.escape(text)}</td>")
            elif key == "PnLPct":
                text = _fmt_pct(row.get(key), blank="--")
                cells.append(f"<td>{html.escape(text)}</td>")
            elif key == "Direction":
                if direction_raw:
                    cells.append(
                        f"<td class='dir {direction_norm}'>{html.escape(direction_raw.upper())}</td>"
                    )
                else:
                    cells.append("<td></td>")
            elif key == "Outcome":
                if outcome_raw:
                    cells.append(
                        f"<td class='result {outcome_norm}'>{html.escape(outcome_raw.upper())}</td>"
                    )
                else:
                    cells.append("<td></td>")
            else:
                text = _safe_text(row.get(key), blank="--")
                cells.append(f"<td>{html.escape(text)}</td>")

        rows_html.append(f"<tr{attr_str}>{''.join(cells)}</tr>")

    body_rows = "".join(rows_html)
    return f"""
    <section class="ops">
        <h2>Detalle Operativo Reciente</h2>
        <table class="ops-table filterable">
            <thead><tr>{header_cells}</tr></thead>
            <tbody>{body_rows}</tbody>
        </table>
    </section>
    """


def _collect_filter_values(series: pd.Series | None) -> list[tuple[str, str]]:
    if series is None:
        return []
    try:
        iterable = series.dropna().unique()
    except Exception:
        iterable = []

    mapping: dict[str, str] = {}
    for raw in iterable:
        label = _safe_text(raw, blank="")
        norm = _normalize_value(raw)
        if not norm:
            continue
        mapping.setdefault(norm, label)

    return sorted(mapping.items(), key=lambda item: item[1].lower())


def build_filters_html(trades: pd.DataFrame) -> str:
    specs = [
        ("direction", "Dirección", trades["Direction"] if "Direction" in trades.columns else None),
        ("entryReason", "Motivo Entrada", trades["EntryReason"] if "EntryReason" in trades.columns else None),
        ("exitReason", "Motivo Salida", trades["ExitReason"] if "ExitReason" in trades.columns else None),
        ("outcome", "Resultado", trades["Outcome"] if "Outcome" in trades.columns else None),
    ]

    controls = []
    for key, label, series in specs:
        options = _collect_filter_values(series)
        options_html = "".join(
            f"<option value='{html.escape(value)}'>{html.escape(display)}</option>"
            for value, display in options
        )
        control_html = f"""
        <label>
            <span>{html.escape(label)}:</span>
            <select data-filter-key="{key}">
                <option value="">Todos</option>
                {options_html}
            </select>
        </label>
        """
        controls.append(control_html)

    controls_html = "".join(controls)
    return f"""
    <section class="filters">
        <h2>Filtrar trades</h2>
        <div class="filters-grid">
            {controls_html}
        </div>
    </section>
    """


def render_dashboard(trades_path: Path, price_path: Path | None, html_out: Path, show: bool, profile: str):
    trades_df = load_trades(trades_path)
    price_df = load_price(price_path)
    summary = summarize_trades(trades_df)

    print("[DASHBOARD] Resumen trades:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    fig = build_figure(trades_df, price_df)
    fig_html = fig.to_html(full_html=False, include_plotlyjs="cdn", config={"displaylogo": False})

    summary_html = build_summary_html(summary)
    filters_html = build_filters_html(trades_df)
    ops_table_html = build_operations_table(trades_df)
    trades_table_html = build_trades_table(trades_df)

    html_out.parent.mkdir(parents=True, exist_ok=True)

    full_html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="utf-8" />
    <title>Dashboard Estrategia Bollinger</title>
    <style>
        body {{
            font-family: 'Inter', Arial, sans-serif;
            background-color: #0f172a;
            color: #f8fafc;
            margin: 0;
            padding: 32px 24px 56px;
        }}
        .hero {{
            display: flex;
            align-items: center;
            gap: 24px;
            margin-bottom: 24px;
        }}
        .hero .logo {{
            flex-shrink: 0;
        }}
        .hero h1 {{
            margin: 0;
            font-size: 1.9rem;
        }}
        .hero p {{
            margin: 6px 0 0;
            color: #94a3b8;
        }}
        h2 {{
            margin-top: 32px;
            border-left: 4px solid #2563eb;
            padding-left: 12px;
            font-size: 1.3rem;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 16px;
            background: #1e293b;
            border-radius: 10px;
            overflow: hidden;
        }}
        th, td {{
            padding: 10px 14px;
            border-bottom: 1px solid #334155;
            text-align: left;
            font-size: 0.95rem;
        }}
        th {{
            color: #60a5fa;
            background: rgba(37, 99, 235, 0.12);
        }}
        .trades-table tbody tr:nth-child(even),
        .ops-table tbody tr:nth-child(even) {{
            background: rgba(15, 23, 42, 0.6);
        }}
        .plot-container {{
            margin-top: 32px;
        }}
        .dir.long {{
            color: #22c55e;
            font-weight: 600;
        }}
        .dir.short {{
            color: #f87171;
            font-weight: 600;
        }}
        .result.win {{
            color: #4ade80;
            font-weight: 600;
        }}
        .result.loss {{
            color: #f87171;
            font-weight: 600;
        }}
        .result.flat {{
            color: #fbbf24;
            font-weight: 600;
        }}
        .filters {{
            margin-top: 32px;
            padding: 18px 20px;
            background: #1e293b;
            border-radius: 10px;
        }}
        .filters h2 {{
            margin: 0 0 12px;
            font-size: 1.2rem;
        }}
        .filters-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 16px 24px;
        }}
        .filters label {{
            display: flex;
            flex-direction: column;
            font-size: 0.9rem;
            color: #cbd5f5;
        }}
        .filters label span {{
            margin-bottom: 6px;
            color: #94a3b8;
            font-weight: 600;
        }}
        .filters select {{
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 6px;
            padding: 6px 10px;
            color: #f8fafc;
            min-width: 160px;
        }}
        .filters select:focus {{
            outline: none;
            border-color: #2563eb;
            box-shadow: 0 0 0 1px #2563eb;
        }}
        @media (max-width: 768px) {{
            .hero {{
                flex-direction: column;
                align-items: flex-start;
            }}
            .hero .logo {{
                margin-bottom: 8px;
            }}
            th, td {{
                font-size: 0.85rem;
            }}
            .filters-grid {{
                flex-direction: column;
            }}
            .filters select {{
                width: 100%;
            }}
        }}
    </style>
</head>
<body>
    <section class="hero">
        <div class="logo">{LOGO_SVG}</div>
        <div>
            <h1>Dashboard Estrategia Bollinger</h1>
            <p>{SYMBOL_DISPLAY} · Intervalo {STREAM_INTERVAL} · Perfil {profile.upper()}</p>
        </div>
    </section>
    {summary_html}
    <div class="plot-container">
        {fig_html}
    </div>
    {filters_html}
    {ops_table_html}
    {trades_table_html}
    <script>
    (function() {{
        const selects = document.querySelectorAll('select[data-filter-key]');
        if (!selects.length) {{
            return;
        }}
        const tables = document.querySelectorAll('table.filterable');

        function applyFilters() {{
            const active = {{}};
            selects.forEach((sel) => {{
                const value = sel.value;
                if (value) {{
                    active[sel.dataset.filterKey] = value;
                }}
            }});

            tables.forEach((table) => {{
                table.querySelectorAll('tbody tr').forEach((row) => {{
                    let visible = true;
                    for (const [key, value] of Object.entries(active)) {{
                        const rowValue = (row.dataset[key] || '');
                        if (rowValue !== value) {{
                            visible = false;
                            break;
                        }}
                    }}
                    row.style.display = visible ? '' : 'none';
                }});
            }});
        }}

        selects.forEach((sel) => sel.addEventListener('change', applyFilters));
        applyFilters();
    }})();
    </script>
</body>
</html>"""

    html_out.write_text(full_html, encoding="utf-8")
    print(f"[DASHBOARD] HTML generado en {html_out}")

    if show:
        webbrowser.open(html_out.resolve().as_uri())


def main():
    parser = argparse.ArgumentParser(description="Dashboard HTML para trades de la estrategia Bollinger.")
    parser.add_argument("--profile", choices=sorted(OUTPUT_PRESETS.keys()), default=None, help="Preset de salidas (tr o historico).")
    parser.add_argument("--trades", type=str, default=None, help="CSV con trades a visualizar.")
    parser.add_argument("--price", type=str, default=str(DEFAULT_PRICE_PATH), help="CSV con precios (ej. alerts_stream.csv).")
    parser.add_argument("--html", type=str, default=None, help="Archivo HTML de salida.")
    parser.add_argument("--show", action="store_true", help="Abrir el dashboard en el navegador al finalizar.")
    args = parser.parse_args()

    profile = resolve_profile(args.profile)
    preset_paths = OUTPUT_PRESETS[profile]

    trades_path = Path(args.trades) if args.trades else preset_paths["trades"]
    price_path = Path(args.price) if args.price else None
    html_path = Path(args.html) if args.html else preset_paths["dashboard"]

    render_dashboard(trades_path, price_path, html_path, args.show, profile)


if __name__ == "__main__":
    main()
