import pandas as pd
import os
from datetime import datetime
from utils import save_data
import re
import pprint

pp = pprint.PrettyPrinter(indent=4)

BASE_RAW_CSV_STOCKS_PATH = f"/usercode/data_lake/raw/csv/stocks/"

dfs = []
for file_name in os.listdir(BASE_RAW_CSV_STOCKS_PATH):
    file_full = f"{BASE_RAW_CSV_STOCKS_PATH}{file_name}"
    file_creation_date = datetime.strptime(file_name.split("_")[0], "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    ticker_name = file_name.split("_")[2].split(".")[0]
    df = pd.read_csv(file_full)
    df.columns = [re.sub(r"\s+", "_", x.lower()) for x in df.columns]
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for item in ["open", "high", "low", "close"]:
        df[item] = df[item].astype(float)
    df["volume"] = df["volume"].astype(int)
    df["ticker_name"] = ticker_name
    df["collect_date"] = file_creation_date
    dfs.append(df)

df = pd.concat(dfs)

save_data(df, "stocks", zone="refined", context="stocks", file_type="parquet")
df_parquet = pd.read_parquet(
    "/usercode/data_lake/refined/parquet/stocks/stocks.parquet"
)
pp.pprint(df_parquet)