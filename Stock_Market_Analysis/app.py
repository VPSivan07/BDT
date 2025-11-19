import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError

# -------------------------------------------------------
# Config
# -------------------------------------------------------
st.set_page_config(page_title="Stock Market Analysis", layout="wide")
st.markdown("<h1 style='text-align:center;'>📊 Stock Market Analysis</h1>", unsafe_allow_html=True)

# -------------------------------------------------------
# GitHub raw base URL (owner/repo/main)
# -------------------------------------------------------
BASE_RAW = "https://raw.githubusercontent.com/VPSivan07/BDT/main/Stock_Market_Analysis/"

# -------------------------------------------------------
# Load data from GitHub (parquet files in repo root)
# -------------------------------------------------------
@st.cache_data
def load_data_from_github(base_url: str = BASE_RAW ):
    """
    Loads all required parquet files from GitHub raw URLs.
    Raises RuntimeError with a helpful message if loading fails.
    """
    filenames = {
        "cleaned": "cleaned.parquet",
        "agg_daily": "agg_daily.parquet",
        "agg_weekly": "agg_weekly.parquet",
        "agg_ticker": "agg_ticker.parquet",
        "agg_sector": "agg_sector.parquet",
        "agg_exchange": "agg_exchange.parquet",
        "agg_notes": "agg_notes.parquet",
    }

    loaded = {}
    for key, fname in filenames.items():
        url = base_url + fname
        try:
            # pd.read_parquet can read from a raw.githubusercontent URL if pyarrow is available.
            loaded[key] = pd.read_parquet(url)
        except Exception as e:
            # provide a clear error with which file failed
            raise RuntimeError(f"Failed to load '{fname}' from GitHub at\n{url}\n\nError: {e}")

    return (
        loaded["cleaned"],
        loaded["agg_daily"],
        loaded["agg_weekly"],
        loaded["agg_ticker"],
        loaded["agg_sector"],
        loaded["agg_exchange"],
        loaded["agg_notes"],
    )


# Try loading and show friendly UI on failure
try:
    cleaned, agg_daily, agg_weekly, agg_ticker, agg_sector, agg_exchange, agg_notes = load_data_from_github()
except RuntimeError as err:
    st.error(
        "Could not load required parquet files from GitHub. Please check the repository name/paths and that parquet files are in the repo root."
    )
    st.caption(str(err))
    st.stop()

# -------------------------------------------------------
# Ensure date-like columns exist and normalize
# -------------------------------------------------------
# If cleaned doesn't have 'trade_date' as datetime, try converting
if "trade_date" in cleaned.columns:
    cleaned["trade_date"] = pd.to_datetime(cleaned["trade_date"], errors="coerce")

# For weekly grouping, create a 'week' column in cleaned if not present
if "week" not in cleaned.columns:
    if "trade_date" in cleaned.columns:
        cleaned["week"] = pd.to_datetime(cleaned["trade_date"]).dt.to_period("W").apply(lambda r: r.start_time)
    else:
        # if no trade_date, create empty week column
        cleaned["week"] = pd.NaT

# Also ensure agg_weekly 'week' is datetime (if it was saved as period/string)
if "week" in agg_weekly.columns:
    try:
        agg_weekly["week"] = pd.to_datetime(agg_weekly["week"], errors="coerce")
    except Exception:
        pass

# If agg_daily has trade_date column, ensure it's datetime
if "trade_date" in agg_daily.columns:
    try:
        agg_daily["trade_date"] = pd.to_datetime(agg_daily["trade_date"], errors="coerce")
    except Exception:
        pass

# -------------------------------------------------------
# Sidebar: Ticker filter
# -------------------------------------------------------
st.sidebar.header("Filters")

# safe-guard: if cleaned has no ticker column, stop with message
if "ticker" not in cleaned.columns:
    st.error("`cleaned.parquet` must contain a 'ticker' column. Please regenerate cleaned.parquet with 02_clean_data.py.")
    st.stop()

available_tickers = sorted(cleaned["ticker"].dropna().unique().astype(str).tolist())
default_selection = available_tickers[:5] if len(available_tickers) >= 5 else available_tickers

tickers = st.sidebar.multiselect("Select Ticker(s)", options=available_tickers, default=default_selection)

# -------------------------------------------------------
# Helpers: merge aggregated tables with cleaned to enable filtering
# -------------------------------------------------------
def merge_agg_with_cleaned_on_date(agg_df, date_col="trade_date"):
    """
    Merge agg_df (which aggregates by date) with cleaned to retain ticker mapping per date,
    so we can filter aggregates to selected tickers.
    Returns agg_df rows that are relevant to selected tickers.
    """
    if date_col not in agg_df.columns:
        # nothing to merge on; return empty
        return pd.DataFrame()
    # deduplicate date->ticker mapping from cleaned
    date_ticker = cleaned[[date_col, "ticker"]].dropna().drop_duplicates()
    merged = agg_df.merge(date_ticker, on=date_col, how="left")
    return merged

# -------------------------------------------------------
# Prepare filtered aggregation DataFrames
# -------------------------------------------------------
# Daily: merge agg_daily with cleaned trade_date -> ticker mapping then filter tickers
if "trade_date" in agg_daily.columns:
    daily_merged = merge_agg_with_cleaned_on_date(agg_daily, "trade_date")
    if tickers:
        agg_daily_f = daily_merged[daily_merged["ticker"].isin(tickers)].copy()
    else:
        agg_daily_f = daily_merged.copy()
else:
    agg_daily_f = pd.DataFrame()

# Weekly: merge agg_weekly with cleaned week -> ticker mapping then filter tickers
if "week" in agg_weekly.columns:
    # ensure cleaned['week'] exists (created earlier)
    week_map = cleaned[["week", "ticker"]].dropna().drop_duplicates()
    weekly_merged = agg_weekly.merge(week_map, on="week", how="left")
    if tickers:
        agg_weekly_f = weekly_merged[weekly_merged["ticker"].isin(tickers)].copy()
    else:
        agg_weekly_f = weekly_merged.copy()
else:
    agg_weekly_f = pd.DataFrame()

# Ticker-level aggregation: simply filter by ticker
if "ticker" in agg_ticker.columns:
    agg_ticker_f = agg_ticker[agg_ticker["ticker"].isin(tickers)] if tickers else agg_ticker.copy()
else:
    agg_ticker_f = pd.DataFrame()

# Sector / Exchange / Notes: filter by sectors/exchanges/notes present for selected tickers
selected_sectors = cleaned[cleaned["ticker"].isin(tickers)]["sector"].dropna().unique() if tickers else cleaned["sector"].dropna().unique()
agg_sector_f = agg_sector[agg_sector["sector"].isin(selected_sectors)] if "sector" in agg_sector.columns else pd.DataFrame()

selected_exchanges = cleaned[cleaned["ticker"].isin(tickers)]["exchange"].dropna().unique() if tickers else cleaned["exchange"].dropna().unique()
agg_exchange_f = agg_exchange[agg_exchange["exchange"].isin(selected_exchanges)] if "exchange" in agg_exchange.columns else pd.DataFrame()

selected_notes = cleaned[cleaned["ticker"].isin(tickers)]["notes"].dropna().unique() if tickers else cleaned["notes"].dropna().unique()
agg_notes_f = agg_notes[agg_notes["notes"].isin(selected_notes)] if "notes" in agg_notes.columns else pd.DataFrame()

# -------------------------------------------------------
# Main Tabs
# -------------------------------------------------------
tabs = st.tabs([
    "Daily Aggregations",
    "Weekly Aggregations",
    "Ticker Aggregations",
    "Sector Aggregations",
    "Exchange Aggregations",
    "Notes Aggregations"
])

# -----------------------------
# Tab 1 — Daily Aggregations
# -----------------------------
with tabs[0]:
    st.subheader("📅 Daily Aggregations")
    if agg_daily_f.empty:
        st.warning("No daily aggregated data available for the selected tickers.")
    else:
        # fold avg_open_price & avg_close_price if present
        value_cols = [c for c in ["avg_open_price", "avg_close_price"] if c in agg_daily_f.columns]
        if not value_cols:
            st.dataframe(agg_daily_f)
        else:
            chart = (
                alt.Chart(agg_daily_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_line(point=True)
                .encode(
                    x=alt.X("trade_date:T", title="Trade Date"),
                    y=alt.Y("Value:Q", title="Price"),
                    color="Metric:N",
                    tooltip=["trade_date"] + value_cols + (["total_volume"] if "total_volume" in agg_daily_f.columns else [])
                )
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_daily_f.reset_index(drop=True).head(200))

# -----------------------------
# Tab 2 — Weekly Aggregations
# -----------------------------
with tabs[1]:
    st.subheader("📊 Weekly Aggregations")
    if agg_weekly_f.empty:
        st.warning("No weekly aggregated data available for the selected tickers.")
    else:
        value_cols = [c for c in ["avg_close_price", "avg_volume"] if c in agg_weekly_f.columns]
        if not value_cols:
            st.dataframe(agg_weekly_f)
        else:
            chart = (
                alt.Chart(agg_weekly_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_line(point=True)
                .encode(
                    x=alt.X("week:T", title="Week Start"),
                    y=alt.Y("Value:Q", title="Value"),
                    color="Metric:N",
                    tooltip=["week"] + value_cols + (["avg_volatility"] if "avg_volatility" in agg_weekly_f.columns else [])
                )
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_weekly_f.reset_index(drop=True).head(200))

# -----------------------------
# Tab 3 — Ticker Aggregations
# -----------------------------
with tabs[2]:
    st.subheader("🔹 Ticker Aggregations")
    if agg_ticker_f.empty:
        st.warning("No ticker-level aggregations available for selected tickers.")
    else:
        value_cols = [c for c in ["avg_open", "avg_close"] if c in agg_ticker_f.columns]
        if not value_cols:
            st.dataframe(agg_ticker_f)
        else:
            chart = (
                alt.Chart(agg_ticker_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_bar()
                .encode(
                    x=alt.X("ticker:N", title="Ticker"),
                    y=alt.Y("Value:Q", title="Price"),
                    color="Metric:N",
                    tooltip=["ticker"] + value_cols + (["price_change_avg"] if "price_change_avg" in agg_ticker_f.columns else [])
                )
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_ticker_f.reset_index(drop=True).head(200))

# -----------------------------
# Tab 4 — Sector Aggregations
# -----------------------------
with tabs[3]:
    st.subheader("🏢 Sector Aggregations")
    if agg_sector_f.empty:
        st.warning("No sector-level data available for selected tickers.")
    else:
        value_cols = [c for c in ["avg_open", "avg_close"] if c in agg_sector_f.columns]
        if not value_cols:
            st.dataframe(agg_sector_f)
        else:
            chart = (
                alt.Chart(agg_sector_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_bar()
                .encode(
                    x=alt.X("sector:N", title="Sector"),
                    y=alt.Y("Value:Q", title="Price"),
                    color="Metric:N",
                    tooltip=["sector"] + value_cols
                )
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_sector_f.reset_index(drop=True).head(200))

# -----------------------------
# Tab 5 — Exchange Aggregations
# -----------------------------
with tabs[4]:
    st.subheader("💱 Exchange Aggregations")
    if agg_exchange_f.empty:
        st.warning("No exchange-level data available for selected tickers.")
    else:
        value_cols = [c for c in ["avg_open", "avg_close"] if c in agg_exchange_f.columns]
        if not value_cols:
            st.dataframe(agg_exchange_f)
        else:
            chart = (
                alt.Chart(agg_exchange_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_bar()
                .encode(
                    x=alt.X("exchange:N", title="Exchange"),
                    y=alt.Y("Value:Q", title="Price"),
                    color="Metric:N",
                    tooltip=["exchange"] + value_cols + (["total_volume"] if "total_volume" in agg_exchange_f.columns else [])
                )
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_exchange_f.reset_index(drop=True).head(200))

# -----------------------------
# Tab 6 — Notes Aggregations
# -----------------------------
with tabs[5]:
    st.subheader("📝 Notes / Events Aggregations")
    if agg_notes_f.empty:
        st.warning("No notes data available for selected tickers.")
    else:
        value_cols = [c for c in ["count", "avg_price_change"] if c in agg_notes_f.columns]
        if not value_cols:
            st.dataframe(agg_notes_f)
        else:
            chart = (
                alt.Chart(agg_notes_f)
                .transform_fold(value_cols, as_=["Metric", "Value"])
                .mark_bar()
                .encode(
                    x=alt.X("notes:N", title="Notes Type"),
                    y=alt.Y("Value:Q", title="Value"),
                    color="Metric:N",
                    tooltip=["notes"] + value_cols
                )
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(agg_notes_f.reset_index(drop=True).head(200))

# -------------------------------------------------------
# Footer
# -------------------------------------------------------
st.markdown(
    """
    <hr>
    <p style='text-align:center; color:gray;'>
        Developed by <b>Vignesh Paramasivam</b>
    </p>
    """,
    unsafe_allow_html=True
)

st.success("✅ Dashboard loaded successfully!")
