import pandas as pd
from .util import Utility, log

ut = Utility()


def preprocess(params):
    records = dict(params)
    records["timestamp"] = ut.yyyy_mm_dd_hh_mm_ss()
    df = pd.DataFrame([records])
    df["timestamp"] = pd.to_datetime(df.timestamp)
    return df


def save(df):
    schema = {
        "timestamp": "TIMESTAMP",
        "created_at": "STRING",
        "location": "STRING",
        "sensor": "STRING",
        "measurements": "FLOAT",
    }
    table = "medaka-iot.v1.water_quality"
    return ut.add_bq(df=df, table=table, schema_dict=schema)


def main(params):
    log(params=params)
    save_df = preprocess(params=params)
    log(save_df=save_df)
    saved = save(save_df)
    log(saved=saved)
    return saved
