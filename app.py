# app.py
import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path

st.set_page_config(page_title="Stock Market Analysis", layout="wide")

st.markdown("<h1 style='text-align:center;'>📊 Stock Market Analysis</h1>", unsafe_allow_html=True)

DATA_DIR = Path(".")
CLEANED = DATA_DIR / "cleaned.parquet"
AGG_DIR = DATA_DIR / "aggregations"

@st.cache_data
def load_cleaned():
    if CLEANED.exists():
        df = pd.read_parquet(CLEANED)
    else:
        st.error(f"cleaned.parquet not found at {CLEANED}. Please run 02_clean_data.py first.")
        return pd.DataFrame()
    # ensure types
    for col in ["open_price", "close_price", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # flags
    df["gap_up_flag"] = df.get("notes", "").astype(str).str.contains("gap up", case=False, na=False).astype(int)
    df["gap_down_flag"] = df.get("notes", "").astype(str).str.contains("gap down", case=False, na=False).astype(int)
    df["validated_flag"] = df.get("validated", "").astype(str).str.lower().isin(["yes", "y"]).astype(int)
    # ensure trade_date parsed
    if "trade_date" in df.columns:
        df["trade_date_parsed"] = pd.to_datetime(df["trade_date"], errors="coerce")
    else:
        df["trade_date_parsed"] = pd.NaT
    return df

@st.cache_data
def compute_aggregations(df: pd.DataFrame):
    out = {}
    # Daily per ticker
    if "trade_date" in df.columns and "ticker" in df.columns:
        daily = df.groupby(["trade_date", "ticker"], as_index=False).agg(
            avg_open_price=("open_price", "mean"),
            avg_close_price=("close_price", "mean"),
            total_volume=("volume", "sum"),
            gap_up_count=("gap_up_flag", "sum"),
            gap_down_count=("gap_down_flag", "sum"),
        )
        out["agg_daily"] = daily
    else:
        out["agg_daily"] = pd.DataFrame()

    # Weekly per ticker
    if "trade_date" in df.columns and "ticker" in df.columns:
        df_loc = df.copy()
        df_loc["week_start"] = pd.to_datetime(df_loc["trade_date"]).dt.to_period("W").apply(lambda r: r.start_time)
        df_loc["volatility"] = df_loc["close_price"] - df_loc["open_price"]
        weekly = df_loc.groupby(["week_start", "ticker"], as_index=False).agg(
            avg_close_price=("close_price", "mean"),
            avg_volume=("volume", "mean"),
            avg_volatility=("volatility", "mean")
        )
        out["agg_weekly"] = weekly
    else:
        out["agg_weekly"] = pd.DataFrame()

    # Ticker agg
    if "ticker" in df.columns:
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
        out["agg_ticker"] = ticker_agg
    else:
        out["agg_ticker"] = pd.DataFrame()

    # Sector
    if "sector" in df.columns:
        sector_agg = df.groupby("sector", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_gap_up=("gap_up_flag", "sum"),
            total_gap_down=("gap_down_flag", "sum")
        )
        out["agg_sector"] = sector_agg
    else:
        out["agg_sector"] = pd.DataFrame()

    # Exchange
    if "exchange" in df.columns:
        exchange_agg = df.groupby("exchange", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_volume=("volume", "sum")
        )
        out["agg_exchange"] = exchange_agg
    else:
        out["agg_exchange"] = pd.DataFrame()

    # Notes
    if "notes" in df.columns:
        notes_agg = df.groupby("notes", as_index=False).agg(
            count=("ticker", "count"),
            avg_price_change=("close_price", lambda x: (x - df.loc[x.index, "open_price"]).mean() if len(x)>0 else None),
            avg_volume=("volume", "mean")
        )
        out["agg_notes"] = notes_agg
    else:
        out["agg_notes"] = pd.DataFrame()

    return out

# Load
df_clean = load_cleaned()
if df_clean.empty:
    st.stop()

aggs = compute_aggregations(df_clean)

# Sidebar filters
st.sidebar.header("Filters")
tickers = st.sidebar.multiselect(
    "Select Ticker(s)",
    options=sorted(df_clean["ticker"].dropna().unique()),
    default=sorted(df_clean["ticker"].dropna().unique())[:5]
)

# Filtered cleaned
filtered_cleaned = df_clean[df_clean["ticker"].isin(tickers)].copy() if tickers else df_clean.copy()

# Recompute filtered aggregation frames for tabs that are recomputed from data
# Use precomputed aggs as baseline, but filter those with ticker columns where present.
agg_daily_f = aggs["agg_daily"]
if "ticker" in agg_daily_f.columns and tickers:
    agg_daily_f = agg_daily_f[agg_daily_f["ticker"].isin(tickers)]

agg_weekly_f = aggs["agg_weekly"]
if "ticker" in agg_weekly_f.columns and tickers:
    agg_weekly_f = agg_weekly_f[agg_weekly_f["ticker"].isin(tickers)]

agg_ticker_f = aggs["agg_ticker"]
if not agg_ticker_f.empty and tickers:
    agg_ticker_f = agg_ticker_f[agg_ticker_f["ticker"].isin(tickers)]

# For sector/exchange/notes we recompute from filtered_cleaned so it's accurate for selected tickers
if not filtered_cleaned.empty:
    if "sector" in filtered_cleaned.columns:
        agg_sector_f = filtered_cleaned.groupby("sector", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_gap_up=("gap_up_flag", "sum"),
            total_gap_down=("gap_down_flag", "sum")
        )
    else:
        agg_sector_f = pd.DataFrame()

    if "exchange" in filtered_cleaned.columns:
        agg_exchange_f = filtered_cleaned.groupby("exchange", as_index=False).agg(
            avg_open=("open_price", "mean"),
            avg_close=("close_price", "mean"),
            total_volume=("volume", "sum")
        )
    else:
        agg_exchange_f = pd.DataFrame()

    if "notes" in filtered_cleaned.columns:
        agg_notes_f = filtered_cleaned.groupby("notes", as_index=False).agg(
            count=("ticker", "count"),
            avg_price_change=("close_price", lambda x: (x - filtered_cleaned.loc[x.index, "open_price"]).mean() if len(x)>0 else None),
            avg_volume=("volume", "mean")
        )
    else:
        agg_notes_f = pd.DataFrame()
else:
    agg_sector_f = pd.DataFrame()
    agg_exchange_f = pd.DataFrame()
    agg_notes_f = pd.DataFrame()

# --- Tabs ---
tabs = st.tabs([
    "Daily Aggregations",
    "Weekly Aggregations",
    "Ticker Aggregations",
    "Sector Aggregations",
    "Exchange Aggregations",
    "Notes Aggregations"
])

# Tab 1 — Daily
with tabs[0]:
    st.subheader("📅 Daily Aggregations")
    if not agg_daily_f.empty:
        chart = alt.Chart(agg_daily_f).transform_fold(
            ["avg_open_price", "avg_close_price"],
            as_=['Metric', 'Value']
        ).mark_line(point=True).encode(
            x=alt.X("trade_date:T", title="Trade Date"),
            y=alt.Y("Value:Q", title="Price"),
            color="Metric:N",
            tooltip=["trade_date", "ticker", "avg_open_price", "avg_close_price", "total_volume"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No daily data for selected ticker(s).")

# Tab 2 — Weekly
with tabs[1]:
    st.subheader("📊 Weekly Aggregations")
    if not agg_weekly_f.empty:
        chart = alt.Chart(agg_weekly_f).transform_fold(
            ["avg_close_price", "avg_volume"],
            as_=['Metric', 'Value']
        ).mark_line(point=True).encode(
            x=alt.X("week_start:T", title="Week"),
            y=alt.Y("Value:Q", title="Value"),
            color="Metric:N",
            tooltip=["week_start", "ticker", "avg_close_price", "avg_volume"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No weekly data for selected ticker(s).")

# Tab 3 — Ticker
with tabs[2]:
    st.subheader("🔹 Ticker Aggregations")
    if not agg_ticker_f.empty:
        chart = alt.Chart(agg_ticker_f).transform_fold(
            ["avg_open", "avg_close"],
            as_=['Metric', 'Value']
        ).mark_bar().encode(
            x=alt.X("ticker:N", title="Ticker"),
            y=alt.Y("Value:Q", title="Price"),
            color="Metric:N",
            tooltip=["ticker", "avg_open", "avg_close", "price_change_avg"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No ticker aggregation data found.")

# Tab 4 — Sector
with tabs[3]:
    st.subheader("🏢 Sector Aggregations")
    if not agg_sector_f.empty:
        chart = alt.Chart(agg_sector_f).transform_fold(
            ["avg_open", "avg_close"],
            as_=['Metric', 'Value']
        ).mark_bar().encode(
            x=alt.X("sector:N", title="Sector"),
            y=alt.Y("Value:Q", title="Price"),
            color="Metric:N",
            tooltip=["sector", "avg_open", "avg_close", "total_gap_up", "total_gap_down"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No sector data for selected tickers.")

# Tab 5 — Exchange
with tabs[4]:
    st.subheader("💱 Exchange Aggregations")
    if not agg_exchange_f.empty:
        chart = alt.Chart(agg_exchange_f).transform_fold(
            ["avg_open", "avg_close"],
            as_=['Metric', 'Value']
        ).mark_bar().encode(
            x=alt.X("exchange:N", title="Exchange"),
            y=alt.Y("Value:Q", title="Price"),
            color="Metric:N",
            tooltip=["exchange", "avg_open", "avg_close", "total_volume"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No exchange data for selected tickers.")

# Tab 6 — Notes
with tabs[5]:
    st.subheader("📝 Notes Aggregations")
    if not agg_notes_f.empty:
        chart = alt.Chart(agg_notes_f).transform_fold(
            ["count", "avg_price_change", "avg_volume"],
            as_=['Metric', 'Value']
        ).mark_bar().encode(
            x=alt.X("notes:N", title="Notes Type"),
            y=alt.Y("Value:Q", title="Value"),
            color="Metric:N",
            tooltip=["notes", "count", "avg_price_change", "avg_volume"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No notes data for selected tickers.")

# Footer
st.markdown("""
<hr>
<p style='text-align:center; color:gray;'>
    Developed by <b>Vignesh Paramasivam</b>
</p>
""", unsafe_allow_html=True)
