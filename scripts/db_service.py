from typing import List
from sqlalchemy import create_engine
import pandas as pd


def get_db_engine():
    # There are dummy local data
    # It does not matter if they
    # are pushed to VSC

    username = "root"
    host = "localhost"
    port = 3306
    database = "bookreco"

    engine = create_engine(
        f"mysql+pymysql://{username}@{host}:{port}/{database}"
    )

    return engine


def save_to_db(dfs: List[pd.DataFrame], tables: List[str]):

    engione = get_db_engine()

    for i, df in enumerate(dfs):
        df.to_sql(tables[i], con=engione)


def read_from_db(table: str):

    engine = get_db_engine()

    try:
        return pd.read_sql(f"SELECT * FROM {table}", con=engine)
    except Exception as e:
        return None
