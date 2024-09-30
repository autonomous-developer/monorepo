import pandas as pd
from datetime import datetime, timezone
from lib.utils import *

URL = <URL Here>

COLUMNS =  [<Column List>]

PROJECT_ID = "<BQ's Project ID>"
TABLE_ID = "<BQ table as dataset.table>"

if __name__ == "__main__":

    data = pd.read_csv(URL, header=1, names=COLUMNS)
    data["report_date"] = pd.to_datetime(data["report_date"])
    data["ingested_at"] = datetime.now(timezone.utc)

    if is_bq_table(PROJECT_ID, TABLE_ID):
        print(f"`{PROJECT_ID}.{TABLE_ID}` exists. Overwriting latest data....")
        start_date = data["report_date"].min().strftime('%Y-%m-%d')
        del_bq_table_data(PROJECT_ID, TABLE_ID, start_date)
        write_to_bq(PROJECT_ID, TABLE_ID, data)
        print("Data Successfully Written")
    else:
        print(f"`{PROJECT_ID}.{TABLE_ID}` not found. Creating new one....")
        write_to_bq(PROJECT_ID, TABLE_ID, data)
        print("Data Successfully Written")
