# 01_load_raw_data.py
import pandas as pd

def main():
    url = "https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/stock_market.csv"
    print("Loading CSV from:", url)
    df = pd.read_csv(url, dtype=str)
    print("Loaded shape:", df.shape)
    print(df.head())

if __name__ == "__main__":
    main()
