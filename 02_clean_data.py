# =============================================================
# STEP 2 — Normalize Schema and Clean Data
# =============================================================

import pandas as pd
import re

# 1. Load the dataset again (from Step 1)
url = "https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/stock_market.csv"
df = pd.read_csv(url, dtype=str)

print("Before cleaning:")
print(df.head(), "\n")

# 2. Convert column headers to snake_case
def to_snake_case(column_name: str) -> str:
    """
    Converts a string to snake_case:
    e.g., "Trade Date" → "trade_date"
    """
    column_name = column_name.strip()                 # Remove leading/trailing spaces
    column_name = re.sub(r'[^0-9a-zA-Z]+', '_', column_name)  # Replace spaces/punct with underscores
    column_name = re.sub(r'_+', '_', column_name)     # Collapse multiple underscores
    return column_name.lower().strip('_')             # Lowercase and remove trailing underscores

df.columns = [to_snake_case(col) for col in df.columns]

print("After snake_case column renaming:")
print(df.columns, "\n")

# 3. Trim whitespace in every cell
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 4. Standardize “missing” tokens to a single consistent value
# Common representations of missing data
missing_tokens = ["", "NA", "N/A", "na", "NaN", "null", "None", "-"]

# Replace all with pandas' built-in missing value indicator (pd.NA)
df.replace(missing_tokens, pd.NA, inplace=True)

# 5. Standardize text casing
for col in df.select_dtypes(include='object').columns:
    if "ticker" in col:
        # Ticker symbols should be uppercase (AAPL, TSLA, etc.)
        df[col] = df[col].astype("string").str.upper()
    else:
        # Other text columns lowercase for consistency
        df[col] = df[col].astype("string").str.lower()

# 6. Fix the date format to yyyy-MM-dd
# Identifying the likely date column (here assumed to be 'trade_date')
if "trade_date" in df.columns:
    df["trade_date"] = pd.to_datetime(
        df["trade_date"], errors="coerce", infer_datetime_format=True
    ).dt.strftime("%Y-%m-%d")

# 7. Final check after cleaning
print("After cleaning:")
print(df.head(), "\n")

print("Schema after normalization:")
print(df.info(), "\n")

print("Null value summary after normalization:")
print(df.isna().sum())
