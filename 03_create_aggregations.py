import pandas as pd

def create_aggregations():
    # 1️⃣ Load cleaned parquet
    df = pd.read_parquet("cleaned.parquet")
    print("Loaded cleaned.parquet:", df.shape)

    # 2️⃣ Ensure numeric columns
    numeric_cols = ["open_price", "close_price", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3️⃣ Normalize exchange for country mapping
    df["exchange_clean"] = df["exchange"].astype(str).str.strip().str.lower()

    df["country"] = df["exchange_clean"].map({
        "nasdaq": "USA",
        "nyse": "USA",
        "lse": "UK",
        "tse": "Japan",
        "hkex": "Hong Kong",
        "tsx": "Canada"
    }).fillna("Unknown")

    # 4️⃣ Flag notes for aggregation
    df["gap_up_flag"] = df["notes"].str.contains("gap up", case=False, na=False).astype(int)
    df["gap_down_flag"] = df["notes"].str.contains("gap down", case=False, na=False).astype(int)
    df["validated_flag"] = df["validated"].str.lower().isin(["yes", "y"]).astype(int)

    # 5️⃣ Daily Aggregation
    daily_agg = df.groupby("trade_date", as_index=False).agg(
        avg_open_price=("open_price", "mean"),
        avg_close_price=("close_price", "mean"),
        total_volume=("volume", "sum"),
        gap_up_count=("gap_up_flag", "sum"),
        gap_down_count=("gap_down_flag", "sum"),
        missing_open=("open_price", lambda x: x.isna().sum()),
        missing_close=("close_price", lambda x: x.isna().sum())
    )
    daily_agg.to_parquet("agg_daily.parquet", index=False)
    print("✅ Saved agg_daily.parquet")

    # 6️⃣ Weekly Aggregation
    df["week"] = pd.to_datetime(df["trade_date"]).dt.to_period("W").apply(lambda r: r.start_time)
    df["volatility"] = df["close_price"] - df["open_price"]

    weekly_agg = df.groupby("week", as_index=False).agg(
        avg_close_price=("close_price", "mean"),
        avg_volume=("volume", "mean"),
        avg_volatility=("volatility", "mean")
    )
    weekly_agg.to_parquet("agg_weekly.parquet", index=False)
    print("✅ Saved agg_weekly.parquet")

    # 7️⃣ Ticker-based Aggregation
    df["price_change"] = df["close_price"] - df["open_price"]

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
    print("✅ Saved agg_ticker.parquet")

    # 8️⃣ Sector-based Aggregation
    sector_agg = df.groupby("sector", as_index=False).agg(
        avg_open=("open_price", "mean"),
        avg_close=("close_price", "mean"),
        total_gap_up=("gap_up_flag", "sum"),
        total_gap_down=("gap_down_flag", "sum")
    )
    sector_agg.to_parquet("agg_sector.parquet", index=False)
    print("✅ Saved agg_sector.parquet")

    # 9️⃣ Exchange-based Aggregation
    exchange_agg = df.groupby("exchange", as_index=False).agg(
        avg_open=("open_price", "mean"),
        avg_close=("close_price", "mean"),
        total_volume=("volume", "sum")
    )
    exchange_agg.to_parquet("agg_exchange.parquet", index=False)
    print("✅ Saved agg_exchange.parquet")

    # 🔟 Notes-based Aggregation
    notes_agg = df.groupby("notes", as_index=False).agg(
        count=("ticker", "count"),
        avg_price_change=("price_change", "mean"),
        avg_volume=("volume", "mean")
    )
    notes_agg.to_parquet("agg_notes.parquet", index=False)
    print("✅ Saved agg_notes.parquet")


# Run aggregations if script is executed directly
if __name__ == "__main__":
    create_aggregations()
