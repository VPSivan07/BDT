# 03_create_aggregations_fixed_v2.py
import pandas as pd
from pathlib import Path

# Use current folder (Stock_Market_Analysis)
CLEANED = Path("cleaned.parquet")

def create_aggregations(cleaned_path=CLEANED):
    df = pd.read_parquet(cleaned_path)

    # -----------------------------
    # Standardize column names
    # -----------------------------
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure required columns exist
    required_cols = ["ticker", "trade_date", "open_price", "close_price", 
                     "volume", "sector", "notes", "validated", "exchange"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # -----------------------------
    # Numeric conversions
    # -----------------------------
    numeric_cols = ["open_price", "close_price", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # -----------------------------
    # Flags
    # -----------------------------
    df["gap_up_flag"] = df["notes"].astype(str).str.contains("gap up", case=False, na=False).astype(int)
    df["gap_down_flag"] = df["notes"].astype(str).str.contains("gap down", case=False, na=False).astype(int)
    df["validated_flag"] = df["validated"].astype(str).str.lower().isin(["yes", "y"]).astype(int)

    # -----------------------------
    # Exchange -> country mapping
    # -----------------------------
    df["exchange_clean"] = df["exchange"].astype(str).str.strip().str.lower()
    df["country"] = df["exchange_clean"].map({
        "nasdaq": "USA",
        "nyse": "USA",
        "lse": "UK",
        "tse": "Japan",
        "hkex": "Hong Kong",
        "tsx": "Canada"
    }).fillna("Unknown")

    # -----------------------------
    # Price change
    # -----------------------------
    df["price_change"] = df["close_price"] - df["open_price"]

    # -----------------------------
    # 1) Daily aggregation
    # -----------------------------
    if "trade_date" in df.columns and "ticker" in df.columns:
        daily = df.groupby(["trade_date", "ticker"], as_index=False).agg(
            avg_open_price=("open_price", "mean"),
            avg_close_price=("close_price", "mean"),
            total_volume=("volume", "sum"),
            gap_up_count=("gap_up_flag", "sum"),
            gap_down_count=("gap_down_flag", "sum"),
            missing_open=("open_price", lambda x: x.isna().sum()),
            missing_close=("close_price", lambda x: x.isna().sum())
        )
        daily.to_parquet("agg_daily.parquet", index=False)
        print("Saved agg_daily.parquet")
    else:
        print("Skipping daily aggregation (missing trade_date or ticker)")

    # -----------------------------
    # 2) Weekly aggregation
    # -----------------------------
    if "trade_date" in df.columns and "ticker" in df.columns:
        df["week_start"] = pd.to_datetime(df["trade_date"]).dt.to_period("W").apply(lambda r: r.start_time)
        df["volatility"] = df["close_price"] - df["open_price"]
        weekly = df.groupby(["week_start", "ticker"], as_index=False).agg(
            avg_close_price=("close_price", "mean"),
            avg_volume=("volume", "mean"),
            avg_volatility=("volatility", "mean")
        )
        weekly.to_parquet("agg_weekly.parquet", index=False)
        print("Saved agg_weekly.parquet")
    else:
        print("Skipping weekly aggregation (missing trade_date or ticker)")

    # -----------------------------
    # 3) Ticker aggregation
    # -----------------------------
    if "ticker" in df.columns:
        ticker_agg = df.groupby("ticker", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            avg_volume=("volume", "mean"),
            price_change_avg=("price_change", "mean"),
            validated_count=("validated_flag", "sum"),
            gap_up_count=("gap_up_flag", "sum"),
            gap_down_count=("gap_down_flag", "sum")
        )
        ticker_agg.to_parquet("agg_ticker.parquet", index=False)
        print("Saved agg_ticker.parquet")

    # -----------------------------
    # 4) Sector aggregation
    # -----------------------------
    if "sector" in df.columns:
        sector_agg = df.groupby("sector", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_gap_up=("gap_up_flag", "sum"),
            total_gap_down=("gap_down_flag", "sum")
        )
        sector_agg.to_parquet("agg_sector.parquet", index=False)
        print("Saved agg_sector.parquet")

    # -----------------------------
    # 5) Exchange aggregation
    # -----------------------------
    if "exchange" in df.columns:
        exchange_agg = df.groupby("exchange", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_volume=("volume", "sum")
        )
        exchange_agg.to_parquet("agg_exchange.parquet", index=False)
        print("Saved agg_exchange.parquet")

    # -----------------------------
    # 6) Notes aggregation
    # -----------------------------
    if "notes" in df.columns:
        notes_agg = df.groupby("notes", as_index=False).agg(
            count=("ticker", "count"),
            avg_price_change=("price_change", "mean"),
            avg_volume=("volume", "mean")
        )
        notes_agg.to_parquet("agg_notes.parquet", index=False)
        print("Saved agg_notes.parquet")


if __name__ == "__main__":
    create_aggregations()
