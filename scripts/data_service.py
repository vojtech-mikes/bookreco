import pandas as pd
import logging
from scripts.etl import load_data
from scripts.corr import calculate_recommendations_corr
from scripts.cos_sim import calculate_recommendations_sim


def create_response(searched: str, typ: str) -> str:
    data, base = load_data()

    response = pd.DataFrame()

    if typ == "corr":
        response = calculate_recommendations_corr(data, searched)
    else:
        response = calculate_recommendations_sim(data, searched)

    response = response.merge(base, on="Book-Title", how="left")

    response = response.drop_duplicates(subset=["Book-Title"])

    response = response[["Title", "Book-Author", "ISBN"]].to_html(
        classes=("uk-table", " uk-table-striped", " uk-table-small"),
        index=False,
    )

    logging.info("Returning HTML response")
    return response
