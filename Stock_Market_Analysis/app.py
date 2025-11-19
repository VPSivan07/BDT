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
# GitHub raw base URL (replace BRANCH with your actual branch name)
# -------------------------------------------------------
BASE_RAW = "https://raw.githubusercontent.com/VPSivan07/BDT/main/Stock_Market_Analysis/"

# -------------------------------------------------------
# Load data from GitHub (parquet files)
# -------------------------------------------------------
@st.cache_data
def load_data_from_github(base_url: str = BASE_RAW):
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
            loaded[key] = pd.read_parquet(url)
        except Exception as e:
            st.warning(f"Failed to load '{fname}' from GitHub at\n{url}\nError: {e}")
            loaded[key] = pd.DataFrame()  # empty DataFrame if load fails
    return (
        loaded["cleaned"],
        loaded["agg_daily"],
        loaded["agg_weekly"],
        loaded["agg_ticker"],
        loaded["agg_sector"],
        loaded["agg_exchange"],
        loaded["agg_notes"],
    )

try:
    cleaned, agg_daily, agg_weekly, agg_ticker, agg_sector, agg_exchange, agg_notes = load_data_from_github()
except Exception as err:
    st.error("Failed to load parquet files.")
    st.stop()

# -------------------------------------------------------
# Normalize columns
# -------------------------------------------------------
# lowercase and strip spaces for safety
def normalize_cols(df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

cleaned = normalize_cols(cleaned)
agg_daily = normalize_cols(agg_daily)
agg_weekly = normalize_cols(agg_weekly)
agg_ticker = normalize_cols(agg_ticker)
agg_sector = normalize_cols(agg_sector)
agg_exchange = normalize_cols(agg_exchange)
agg_notes = normalize_cols(agg_notes)

# Ensure date columns
if "trade_date" in cleaned.columns:
    cleaned["trade_date"] = pd.to_datetime(cleaned["trade_date"], errors="coerce")

if "week" not in cleaned.columns and "trade_date" in cleaned.columns:
    cleaned["week"] = cleaned["trade_date"].dt.to_period("W").apply(lambda r: r.start_time)

if "week" in agg_weekly.columns:
    agg_weekly["week"] = pd.to_datetime(agg_weekly["week"], errors="coerce")

if "trade_date" in agg_daily.columns:
    agg_daily["trade_date"] = pd.to_datetime(agg_daily["trade_date"], errors="coerce")

# -------------------------------------------------------
# Sidebar Filters
# -------------------------------------------------------
st.sidebar.header("Filters")
if "ticker" not in cleaned.columns:
    st.error("`cleaned.parquet` must contain a 'ticker' column.")
    st.stop()

available_tickers = sorted(cleaned["ticker"].dropna().unique().astype(str).tolist())
default_selection = available_tickers[:5] if len(available_tickers) >= 5 else available_tickers
tickers = st.sidebar.multiselect("Select Ticker(s)", options=available_tickers, default=default_selection)

# -------------------------------------------------------
# Merge helper
# -------------------------------------------------------
def merge_agg_with_cleaned_on_date(agg_df, date_col="trade_date"):
    if date_col not in agg_df.columns:
        st.warning(f"{date_col} not found in aggregate DataFrame.")
        return pd.DataFrame()
    if "ticker" not in cleaned.columns:
        st.warning("'ticker' column missing in cleaned.parquet")
        return pd.DataFrame()
    date_ticker = cleaned[[date_col, "ticker"]].dropna().drop_duplicates()
    merged = agg_df.merge(date_ticker, on=date_col, how="left")
    return merged

# -------------------------------------------------------
# Filtered DataFrames
# -------------------------------------------------------
# Daily
if "trade_date" in agg_daily.columns:
    daily_merged = merge_agg_with_cleaned_on_date(agg_daily, "trade_date")
    if "ticker" in daily_merged.columns:
        agg_daily_f = daily_merged[daily_merged["ticker"].isin(tickers)].copy()
    else:
        agg_daily_f = pd.DataFrame()
else:
    agg_daily_f = pd.DataFrame()

# Weekly
if "week" in agg_weekly.columns:
    week_map = cleaned[["week", "ticker"]].dropna().drop_duplicates()
    weekly_merged = agg_weekly.merge(week_map, on="week", how="left")
    if "ticker" in weekly_merged.columns:
        agg_weekly_f = weekly_merged[weekly_merged["ticker"].isin(tickers)].copy()
    else:
        agg_weekly_f = pd.DataFrame()
else:
    agg_weekly_f = pd.DataFrame()

# Ticker-level
if "ticker" in agg_ticker.columns:
    agg_ticker_f = agg_ticker[agg_ticker["ticker"].isin(tickers)] if tickers else agg_ticker.copy()
else:
    agg_ticker_f = pd.DataFrame()

# Sector / Exchange / Notes
selected_sectors = cleaned[cleaned["ticker"].isin(tickers)]["sector"].dropna().unique() if tickers else cleaned["sector"].dropna().unique()
agg_sector_f = agg_sector[agg_sector["sector"].isin(selected_sectors)] if "sector" in agg_sector.columns else pd.DataFrame()

selected_exchanges = cleaned[cleaned["ticker"].isin(tickers)]["exchange"].dropna().unique() if tickers else cleaned["exchange"].dropna().unique()
agg_exchange_f = agg_exchange[agg_exchange["exchange"].isin(selected_exchanges)] if "exchange" in agg_exchange.columns else pd.DataFrame()

selected_notes = cleaned[cleaned["ticker"].isin(tickers)]["notes"].dropna().unique() if tickers else cleaned["notes"].dropna().unique()
agg_notes_f = agg_notes[agg_notes["notes"].isin(selected_notes)] if "notes" in agg_notes.columns else pd.DataFrame()

# -------------------------------------------------------
# Tabs (keep your existing tab code as-is)
# -------------------------------------------------------
tabs = st.tabs([
    "Daily Aggregations",
    "Weekly Aggregations",
    "Ticker Aggregations",
    "Sector Aggregations",
    "Exchange Aggregations",
    "Notes Aggregations"
])

# The rest of your tab code can remain exactly the same as before,
# since agg_daily_f, agg_weekly_f, etc., are now safely created.

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
