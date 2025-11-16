# 02_clean_data.py
import pandas as pd
import re
import sys
from pathlib import Path

SRC = Path("stock_market.csv")
OUT = Path("cleaned.parquet")

def to_snake_case(s: str) -> str:
    s = s.strip()
    s = re.sub(r'[^0-9a-zA-Z]+', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.lower().strip('_')

def main(csv_path: Path = SRC, out_path: Path = OUT):
    if not csv_path.exists():
        print(f"CSV file not found at {csv_path}. Provide the CSV or change the path.")
        sys.exit(1)

    df = pd.read_csv(csv_path, dtype=str)
    # Normalize columns
    df.columns = [to_snake_case(c) for c in df.columns]

    # Trim whitespace in string cells
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Standardize missing tokens
    missing_tokens = ["", "NA", "N/A", "na", "NaN", "null", "None", "-"]
    df.replace(missing_tokens, pd.NA, inplace=True)

    # Standardize cases
    for col in df.select_dtypes(include='object').columns:
        if "ticker" in col:
            df[col] = df[col].astype("string").str.upper()
        else:
            df[col] = df[col].astype("string").str.lower()

    # Parse trade_date (if exists)
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(
            df["trade_date"], errors="coerce", infer_datetime_format=True
        ).dt.strftime("%Y-%m-%d")

    # Save cleaned parquet (requires pyarrow)
    df.to_parquet(out_path, index=False)
    print("Saved cleaned data to", out_path)

if __name__ == "__main__":
    main()
