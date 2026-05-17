import os
import pandas as pd

from dotenv import load_dotenv

from paginado_binance import fetch_klines_paginado


load_dotenv()

SYMBOL_DISPLAY = os.getenv("SYMBOL", "ETHUSDT.P")
API_SYMBOL = SYMBOL_DISPLAY.replace(".P", "")

STREAM_INTERVAL = os.getenv("STREAM_INTERVAL", "30m").strip()
BB_LENGTH = int(os.getenv("BB_LENGTH", "20"))
BB_MULT = float(os.getenv("BB_MULT", "2.0"))
BB_DIRECTION = int(os.getenv("BB_DIRECTION", "0"))
try:
    BB_STD_DDOF = int(os.getenv("BB_STD_DDOF", "1"))
except ValueError:
    BB_STD_DDOF = 1
BB_STD_DDOF = max(BB_STD_DDOF, 0)


def fetch_stream_ohlc(limit: int) -> pd.DataFrame:
    df = fetch_klines_paginado(API_SYMBOL, STREAM_INTERVAL, limit)
    if df.empty:
        return df
    return df[["Open", "High", "Low", "Close", "Volume"]]


def compute_bollinger_bands(df: pd.DataFrame, length: int, mult: float) -> pd.DataFrame:
    if df is None or df.empty:
        idx = df.index if df is not None else None
        return pd.DataFrame(index=idx)

    length = max(int(length), 1)
    mult = float(mult)

    close = df["Close"].astype("float64")
    basis = close.rolling(length, min_periods=1).mean()
    deviation = close.rolling(length, min_periods=1).std(ddof=BB_STD_DDOF)
    upper = basis + mult * deviation
    lower = basis - mult * deviation

    idx = df.index
    return pd.DataFrame(
        {
            "basis": basis,
            "upper": upper,
            "lower": lower,
            "deviation": deviation,
            "close": close,
        },
        index=idx,
    )


def main():
    df = fetch_stream_ohlc(5000)
    bb = compute_bollinger_bands(df, BB_LENGTH, BB_MULT)
    if df.empty or bb.empty:
        print("[WARN] No se pudieron obtener datos.")
        return
    last = bb.iloc[-1]
    print("Última vela:", bb.index[-1])
    print("Bollinger Basis:", last.get("basis"))
    print("Bollinger Upper:", last.get("upper"))
    print("Bollinger Lower:", last.get("lower"))


if __name__ == "__main__":
    main()
