import streamlit as st
import pandas as pd
import altair as alt

# -------------------------------------------------------
# Config
# -------------------------------------------------------
st.set_page_config(page_title="Stock Market Analysis", layout="wide")
st.markdown("<h1 style='text-align:center;'>📊 Stock Market Analysis</h1>", unsafe_allow_html=True)

# -------------------------------------------------------
# GitHub raw base URL
# -------------------------------------------------------
BASE_RAW = "https://raw.githubusercontent.com/VPSivan07/BDT/main/Stock_Market_Analysis/"

# -------------------------------------------------------
# Load data
# -------------------------------------------------------
@st.cache_data
def load_parquets(base_url=BASE_RAW):
    files = ["cleaned", "agg_daily", "agg_weekly", "agg_ticker", 
             "agg_sector", "agg_exchange", "agg_notes"]
    loaded = {}
    for f in files:
        url = f"{base_url}{f}.parquet"
        try:
            df = pd.read_parquet(url)
            # Normalize column names
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            loaded[f] = df
        except Exception as e:
            st.warning(f"Could not load {f}.parquet: {e}")
            loaded[f] = pd.DataFrame()
    return loaded

data = load_parquets()
cleaned = data["cleaned"]
agg_daily = data["agg_daily"]
agg_weekly = data["agg_weekly"]
agg_ticker = data["agg_ticker"]
agg_sector = data["agg_sector"]
agg_exchange = data["agg_exchange"]
agg_notes = data["agg_notes"]

# -------------------------------------------------------
# Normalize tickers
# -------------------------------------------------------
def normalize_ticker(df):
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    return df

for df_name in ["cleaned", "agg_daily", "agg_weekly", "agg_ticker"]:
    data[df_name] = normalize_ticker(data[df_name])

cleaned = data["cleaned"]
agg_daily = data["agg_daily"]
agg_weekly = data["agg_weekly"]
agg_ticker = data["agg_ticker"]

# -------------------------------------------------------
# Normalize date columns
# -------------------------------------------------------
if "trade_date" in cleaned.columns:
    cleaned["trade_date"] = pd.to_datetime(cleaned["trade_date"], errors="coerce").dt.date
if "trade_date" in agg_daily.columns:
    agg_daily["trade_date"] = pd.to_datetime(agg_daily["trade_date"], errors="coerce").dt.date
if "week_start" in agg_weekly.columns:
    agg_weekly["week_start"] = pd.to_datetime(agg_weekly["week_start"], errors="coerce")

# -------------------------------------------------------
# Sidebar: Ticker filter
# -------------------------------------------------------
st.sidebar.header("Filters")
if "ticker" not in cleaned.columns:
    st.error("`cleaned.parquet` must contain 'ticker' column")
    st.stop()

available_tickers = sorted(cleaned["ticker"].dropna().unique())
default_selection = available_tickers[:5] if len(available_tickers) >= 5 else available_tickers
tickers = st.sidebar.multiselect("Select ticker(s)", options=available_tickers, default=default_selection)

# -------------------------------------------------------
# Filtered DataFrames
# -------------------------------------------------------

# --- Daily ---
agg_daily_f = agg_daily.copy()
if "ticker" in agg_daily_f.columns and tickers:
    agg_daily_f = agg_daily_f[agg_daily_f["ticker"].isin(tickers)]

# --- Weekly ---
agg_weekly_f = agg_weekly.copy()
if "ticker" in agg_weekly_f.columns and "week_start" in agg_weekly_f.columns and tickers:
    # Filter by selected tickers
    agg_weekly_f = agg_weekly_f[agg_weekly_f["ticker"].isin(tickers)]

# --- Ticker ---
agg_ticker_f = agg_ticker.copy()
if "ticker" in agg_ticker_f.columns and tickers:
    agg_ticker_f = agg_ticker_f[agg_ticker_f["ticker"].isin(tickers)]

# --- Sector ---
agg_sector_f = pd.DataFrame()
if "sector" in agg_sector.columns and "sector" in cleaned.columns:
    selected_sectors = cleaned[cleaned["ticker"].isin(tickers)]["sector"].dropna().unique()
    agg_sector_f = agg_sector[agg_sector["sector"].isin(selected_sectors)]

# --- Exchange ---
agg_exchange_f = pd.DataFrame()
if "exchange" in agg_exchange.columns and "exchange" in cleaned.columns:
    selected_exchanges = cleaned[cleaned["ticker"].isin(tickers)]["exchange"].dropna().unique()
    agg_exchange_f = agg_exchange[agg_exchange["exchange"].isin(selected_exchanges)]

# --- Notes ---
agg_notes_f = pd.DataFrame()
if "notes" in agg_notes.columns and "notes" in cleaned.columns:
    selected_notes = cleaned[cleaned["ticker"].isin(tickers)]["notes"].dropna().unique()
    agg_notes_f = agg_notes[agg_notes["notes"].isin(selected_notes)]

# -------------------------------------------------------
# Tabs
# -------------------------------------------------------
tabs = st.tabs([
    "Daily Aggregations",
    "Weekly Aggregations",
    "Ticker Aggregations",
    "Sector Aggregations",
    "Exchange Aggregations",
    "Notes Aggregations"
])

# --- Daily Tab ---
with tabs[0]:
    st.subheader("📅 Daily Aggregations")
    if agg_daily_f.empty:
        st.warning("No daily data available")
    else:
        value_cols = [c for c in ["avg_open_price", "avg_close_price"] if c in agg_daily_f.columns]
        if value_cols:
            chart = alt.Chart(agg_daily_f).transform_fold(
                value_cols, as_=["Metric", "Value"]
            ).mark_line(point=True).encode(
                x=alt.X("trade_date:T", title="Trade Date"),
                y=alt.Y("Value:Q", title="Price"),
                color="Metric:N",
                tooltip=["trade_date"] + value_cols
            ).interactive()
            st.altair_chart(chart, use_container_width=True)
        st.dataframe(agg_daily_f.head(200))

# --- Weekly Tab ---
with tabs[1]:
    st.subheader("📊 Weekly Aggregations")
    if agg_weekly_f.empty:
        st.warning("No weekly data available")
    else:
        value_cols = [c for c in ["avg_close_price", "avg_volume"] if c in agg_weekly_f.columns]
        if value_cols:
            chart = alt.Chart(agg_weekly_f).transform_fold(
                value_cols, as_=["Metric", "Value"]
            ).mark_line(point=True).encode(
                x=alt.X("week_start:T", title="Week Start"),
                y=alt.Y("Value:Q", title="Value"),
                color="Metric:N",
                tooltip=["week_start"] + value_cols
            ).interactive()
            st.altair_chart(chart, use_container_width=True)
        st.dataframe(agg_weekly_f.head(200))

# --- Ticker Tab ---
with tabs[2]:
    st.subheader("💹 Ticker Aggregations")
    if agg_ticker_f.empty:
        st.warning("No ticker-level data available")
    else:
        st.dataframe(agg_ticker_f.head(200))

# --- Sector Tab ---
with tabs[3]:
    st.subheader("🏢 Sector Aggregations")
    if agg_sector_f.empty:
        st.warning("No sector-level data available")
    else:
        st.dataframe(agg_sector_f.head(200))

# --- Exchange Tab ---
with tabs[4]:
    st.subheader("🌎 Exchange Aggregations")
    if agg_exchange_f.empty:
        st.warning("No exchange-level data available")
    else:
        st.dataframe(agg_exchange_f.head(200))

# --- Notes Tab ---
with tabs[5]:
    st.subheader("📝 Notes Aggregations")
    if agg_notes_f.empty:
        st.warning("No notes-level data available")
    else:
        st.dataframe(agg_notes_f.head(200))

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
