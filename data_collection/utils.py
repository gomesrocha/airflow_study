import pandas as pd
import json
import os
from typing import Dict, List, Union
from datetime import datetime, timedelta


def save_data(
    file_content: Union[List[Dict], Dict, List, str, pd.DataFrame],
    file_name: str,
    zone: str = "raw",
    context: str = "books",
    file_type: str = "csv",
    base_path="/usercode/data_lake/",
) -> None:
    DATA_LAKE_BASE_PATH = f"{base_path}{zone}/{file_type}/{context}/"

    full_file_name = f"{DATA_LAKE_BASE_PATH}{file_name}"
    if file_type == "csv" and zone == "raw":
        if not isinstance(file_content, pd.DataFrame):
            df = pd.DataFrame(file_content)
        else:
            df = file_content
        df.to_csv(f"{full_file_name}.csv", index=False)
    elif file_type == "json" and zone == "raw":
        with open(f"{full_file_name}.json", "w") as fp:
            json.dump(file_content, fp)
    elif file_type == "parquet" and zone == "refined":
        if not isinstance(file_content, pd.DataFrame):
            df = pd.DataFrame(file_content)
        else:
            df = file_content
        cols_except_dt = [c for c in df.columns.tolist() if c != "collect_date"]
        df = df.sort_values("collect_date", ascending=False).drop_duplicates(
            subset=cols_except_dt, keep="last"
        )
        df.to_parquet(f"{full_file_name}.parquet")
    else:
        print(
            "Specified file type not found or combination of Zone and File Type does not match"
        )


def get_missing_stock_dates(
    df: pd.DataFrame, ticker: str, date_col_name: str, start_date: str, end_date: str
) -> List[str]:
    df_filtered = df.query("ticker_name == @ticker")
    dt_fmt = "%Y-%m-%d"
    if df_filtered.shape[0] == 0:
        raise Exception("ticker does not exist in the data")

    existing_dates = df_filtered[date_col_name].unique().tolist()
    datetime_start_date = datetime.strptime(start_date, dt_fmt)
    datetime_end_date = datetime.strptime(end_date, dt_fmt)
    expected_dates = [
        (datetime_start_date + timedelta(days=x)).strftime(dt_fmt)
        for x in range((datetime_end_date - datetime_start_date).days)
    ] + [end_date]
    return list(set(expected_dates) - set(existing_dates))

