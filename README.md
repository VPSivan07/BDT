# Stock Market Streamlit Dashboard

Developed by Vignesh Paramasivam

This project is a **Streamlit-based interactive dashboard** for analyzing stock market data. It includes cleaned data, aggregated metrics, and visualizations across **daily, weekly, ticker, sector, exchange, and notes-based perspectives**.  

The dashboard is designed to help traders, analysts, or researchers quickly explore market trends and understand stock performance.

## Prerequisites
- Python **3.12.x** (recommended 3.12.13)
- [uv](https://pypi.org/project/uv/) installed for dependency management (or your chosen UV client)
- VSCode (optional) with Python extension

## Install UV (one-time)
```bash
pip install uv

## Features

1. **Interactive Ticker Filtering**  
   - Select one or multiple tickers to focus on specific stocks.  
   - All tabs dynamically update according to your selection.

2. **Multiple Aggregation Views**  
   - **Daily Aggregations**: Average open/close prices, total volume, daily gaps.  
   - **Weekly Aggregations**: Weekly average close price, volume, volatility.  
   - **Ticker Aggregations**: Per-ticker averages, price changes, validated count, gap counts.  
   - **Sector Aggregations**: Average prices and total gaps per sector.  
   - **Exchange Aggregations**: Aggregated metrics by exchange.  
   - **Notes Aggregations**: Summary statistics based on notes (e.g., gap up/down events).

3. **Data Cleaning and Normalization**  
   - Columns converted to `snake_case`.  
   - Whitespace removed, missing values standardized.  
   - Ticker symbols normalized to uppercase; other text to lowercase.  
   - Date column formatted as `YYYY-MM-DD`.  

4. **Responsive Charts with Tooltips**  
   - Built with **Altair**.  
   - Interactive points, hover tooltips, and fold transformations for multi-metric comparison.