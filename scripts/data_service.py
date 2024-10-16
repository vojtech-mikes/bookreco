import pandas as pd
import logging
from scripts.etl import load_data
from scripts.corr import calculate_recommendations


def create_response(searched: str) -> str:
    data, base = load_data()

    response = calculate_recommendations(data, searched)

    response = response.merge(base, on="Book-Title", how="left")

    response = response.drop_duplicates(subset=["Book-Title"])

    response = response[["Title", "Book-Author", "ISBN"]].to_html(
        classes=("uk-table", " uk-table-striped", " uk-table-small"),
        index=False,
    )

    logging.info("Returning HTML response")
    return response
