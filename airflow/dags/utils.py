import os
import logging
from datetime import datetime 

import pandas as pd
import sqlalchemy as sqla


# DEFAULT_PATH = "/tmp/data/" # from docker compose

DB_POSTGRES_USER = os.environ.get('DB_POSTGRES_USER')
DB_POSTGRES_PASSWORD = os.environ.get('DB_POSTGRES_PASSWORD')
DB_POSTGRES_DB = os.environ.get('DB_POSTGRES_DB')
DB_POSTGRES_HOST = "db" # taken from  docker compose
DB_POSTGRES_PORT = 5432 # taken from  docker compose

PRICES_TABLE = "prices"
METRTRIC_TABLE = "price_metrics"
DIM_METRIC_TABLE = "dim_metrics"


# -----------------------------------------------------------------------------

class DataFrameDescriptor:

    def __init__(self, df: pd.DataFrame):
        self._df = df
    
    def description(self, col: str, desc: str):
        ds = self._df[col]
        if desc == "min":
            return float(ds.min())
        if desc == "max":
            return float(ds.max())
        if desc == "count":
            return float(ds.count())
        if desc == "average":
            return float(ds.mean())
        if desc == "sum":
            return float(ds.sum())
        if desc == "median":
            return float(ds.median())
        if desc == "stddev":
            return float(ds.std())

# -----------------------------------------------------------------------------

def create_filepath(path, ds=None):
    logging.info("ds value:")
    logging.info(ds)
    if ds == None:
        return path
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    ds_date_final = ds_date.strftime("%Y-%m.csv").replace("-0", "-") # remove zero padding
    return os.path.join(path, ds_date_final)

# -----------------------------------------------------------------------------

def function_check_file(path, ds=None):
    if ds == None:
        return os.path.isfile(path)
    return os.path.isfile(create_filepath(path, ds))

# -----------------------------------------------------------------------------

def function_migrate_data(path, ds=None):
    # 
    sql_uri = f"postgresql://{DB_POSTGRES_USER}:{DB_POSTGRES_PASSWORD}@{DB_POSTGRES_HOST}:{DB_POSTGRES_PORT}/{DB_POSTGRES_DB}"
    engine = sqla.create_engine(sql_uri)
    # 
    filepath = create_filepath(path, ds)
    df = pd.read_csv(filepath, index_col=False)
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=False) # .df.strftime('%m/%d/%Y')
    df.to_sql(PRICES_TABLE, con=engine, if_exists='append', index=False)
    
    engine.dispose()

# -----------------------------------------------------------------------------

def function_metric_data(path, ds=None, use_pandas=True):
    
    filepath = create_filepath(path, ds)
    df = pd.read_csv(filepath, index_col=False)
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=False) # .df.strftime('%m/%d/%Y')
    df_descriptor = DataFrameDescriptor(df)
    # 
    sql_uri = f"postgresql://{DB_POSTGRES_USER}:{DB_POSTGRES_PASSWORD}@{DB_POSTGRES_HOST}:{DB_POSTGRES_PORT}/{DB_POSTGRES_DB}"
    engine = sqla.create_engine(sql_uri)
    connection = engine.connect()

    metadata = sqla.MetaData()
    metric_table = sqla.Table(METRTRIC_TABLE, metadata, autoload_with=engine)

    # get list of metrics
    dim_metric_table = sqla.Table(DIM_METRIC_TABLE, metadata, autoload_with=engine)
    stmt = sqla.select(dim_metric_table)
    exe = connection.execute(stmt)
    results = exe.fetchall()

    records = []

    ds_date = None
    if ds is None:
        ds_date = df['timestamp'].max().to_pydatetime()
    else:
        ds_date = datetime.strptime(ds, "%Y-%m-%d")

    for id, name in results:
        # print(id, name)
        metric = df_descriptor.description(col="price", desc=name)
        logging.info({ "timestamp": ds_date, "metric_type": id, "metric_value": metric })
        
        if use_pandas:
            records.append({ "timestamp": ds_date, "metric_type": id, "metric_value": metric })
            continue

        try:
            ins_stmt = metric_table.insert()\
                    .values(timestamp=ds_date, metric_type=id, metric_value=metric)\
                    .return_defaults()
            connection.execute(ins_stmt)
            connection.commit()
        except Exception as e:
            logging.error("ERROR:")
            logging.error(e)
            print(e)

    if use_pandas:
        df_db = pd.DataFrame.from_records(records)
        df_db.to_sql(name=METRTRIC_TABLE, con=engine, if_exists="append", index=False)

    engine.dispose()

# -----------------------------------------------------------------------------
