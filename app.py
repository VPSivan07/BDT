import streamlit as st
import pandas as pd
import altair as alt

# -------------------------------------------------------
# Page Configuration
# -------------------------------------------------------
st.set_page_config(
    page_title="Stock Market Analysis",
    layout="wide"
)

st.markdown(
    "<h1 style='text-align:center;'>📊 Stock Market Analysis</h1>",
    unsafe_allow_html=True
)

# -------------------------------------------------------
# Load Data
# -------------------------------------------------------
@st.cache_data
def load_data():
    cleaned = pd.read_parquet("cleaned.parquet")
    agg_daily = pd.read_parquet("agg_daily.parquet")
    agg_weekly = pd.read_parquet("agg_weekly.parquet")
    agg_ticker = pd.read_parquet("agg_ticker.parquet")
    agg_sector = pd.read_parquet("agg_sector.parquet")
    agg_exchange = pd.read_parquet("agg_exchange.parquet")
    agg_notes = pd.read_parquet("agg_notes.parquet")
    return cleaned, agg_daily, agg_weekly, agg_ticker, agg_sector, agg_exchange, agg_notes

cleaned, agg_daily, agg_weekly, agg_ticker, agg_sector, agg_exchange, agg_notes = load_data()

# -------------------------------------------------------
# Sidebar Filters
# -------------------------------------------------------
st.sidebar.header("Filters")

tickers = st.sidebar.multiselect(
    "Select Ticker(s)",
    options=cleaned["ticker"].dropna().unique(),
    default=cleaned["ticker"].dropna().unique()[:5]
)

# Filter cleaned data by selected tickers
filtered_cleaned = cleaned[cleaned["ticker"].isin(tickers)].copy()

# -------------------------------------------------------
# Preprocess filtered_cleaned for flags and numeric columns
# -------------------------------------------------------
filtered_cleaned["gap_up_flag"] = filtered_cleaned["notes"].str.contains("gap up", case=False, na=False).astype(int)
filtered_cleaned["gap_down_flag"] = filtered_cleaned["notes"].str.contains("gap down", case=False, na=False).astype(int)
filtered_cleaned["validated_flag"] = filtered_cleaned["validated"].str.lower().isin(["yes", "y"]).astype(int)

numeric_cols = ["open_price", "close_price", "volume"]
for col in numeric_cols:
    filtered_cleaned[col] = pd.to_numeric(filtered_cleaned[col], errors="coerce")

# -------------------------------------------------------
# Filter Aggregations by Selected Tickers
# -------------------------------------------------------
# Only filter ticker-level aggregation
agg_ticker_f = agg_ticker[agg_ticker["ticker"].isin(tickers)]

# Daily & weekly aggregations: filter if ticker exists in columns
agg_daily_f = agg_daily[agg_daily["ticker"].isin(tickers)] if "ticker" in agg_daily.columns else agg_daily
agg_weekly_f = agg_weekly[agg_weekly["ticker"].isin(tickers)] if "ticker" in agg_weekly.columns else agg_weekly

# Sector aggregation: recompute from filtered_cleaned
agg_sector_f = filtered_cleaned.groupby("sector", as_index=False).agg(
    avg_open=("open_price", "mean"),
    avg_close=("close_price", "mean"),
    total_gap_up=("gap_up_flag", "sum"),
    total_gap_down=("gap_down_flag", "sum")
)

# Exchange aggregation: recompute from filtered_cleaned
agg_exchange_f = filtered_cleaned.groupby("exchange", as_index=False).agg(
    avg_open=("open_price", "mean"),
    avg_close=("close_price", "mean"),
    total_volume=("volume", "sum")
)

# Notes aggregation: recompute from filtered_cleaned
agg_notes_f = filtered_cleaned.groupby("notes", as_index=False).agg(
    count=("ticker", "count"),
    avg_price_change=("close_price", lambda x: (x - filtered_cleaned.loc[x.index, "open_price"]).mean()),
    avg_volume=("volume", "mean")
)

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

# -------------------------------------------------------
# Tab 1 — Daily Aggregations
# -------------------------------------------------------
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
            tooltip=["trade_date", "avg_open_price", "avg_close_price", "total_volume"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No daily data for selected ticker(s).")

# -------------------------------------------------------
# Tab 2 — Weekly Aggregations
# -------------------------------------------------------
with tabs[1]:
    st.subheader("📊 Weekly Aggregations")

    if not agg_weekly_f.empty:
        chart = alt.Chart(agg_weekly_f).transform_fold(
            ["avg_close_price", "avg_volume"],
            as_=['Metric', 'Value']
        ).mark_line(point=True).encode(
            x=alt.X("week:T", title="Week"),
            y=alt.Y("Value:Q", title="Value"),
            color="Metric:N",
            tooltip=["week", "avg_close_price", "avg_volume"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No weekly data for selected ticker(s).")

# -------------------------------------------------------
# Tab 3 — Ticker Aggregations
# -------------------------------------------------------
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

# -------------------------------------------------------
# Tab 4 — Sector Aggregations
# -------------------------------------------------------
with tabs[3]:
    st.subheader("🏢 Sector Aggregations")

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

# -------------------------------------------------------
# Tab 5 — Exchange Aggregations
# -------------------------------------------------------
with tabs[4]:
    st.subheader("💱 Exchange Aggregations")

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

# -------------------------------------------------------
# Tab 6 — Notes Aggregations
# -------------------------------------------------------
with tabs[5]:
    st.subheader("📝 Notes Aggregations")

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
