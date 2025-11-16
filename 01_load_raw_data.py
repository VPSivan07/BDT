# =============================================================
# STEP 1 — Load and Inspect Raw Stock Market Data
# =============================================================

# Import libraries
import pandas as pd

# 1. Load the CSV directly from GitHub raw link
url = "https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/stock_market.csv"

# Read the CSV file into a pandas DataFrame
# Using 'dtype=str' to initially treat everything as text
df = pd.read_csv(url, dtype=str)

# 2. Inspect basic structure
print("DATA LOADED SUCCESSFULLY")
print("=" * 60)

# Shape gives (rows, columns)
print("Shape of dataset: {df.shape[0]} rows × {df.shape[1]} columns\n")

# 3. Preview first few rows
print("Preview of first 5 rows:")
print(df.head(), "\n")

# 4. Check column info and data types
print("Schema Information:")
print(df.info(), "\n")

# 5. Summary of missing/null values
print("Null / Missing Value Summary:")
print(df.isna().sum(), "\n")

# 6. Showing percentage of missing values per column
print("🔹 Percentage of Missing Values:")
print((df.isna().sum() / len(df) * 100).round(2).astype(str) + "%")

