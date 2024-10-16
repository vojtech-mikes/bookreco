from pathlib import Path
import pandas as pd
import logging


def load_data():

    parquet_pth = Path("results/dataset.parquet")
    base_parquet_pth = Path("results/baseset.parquet")

    if parquet_pth.is_file() and base_parquet_pth.is_file():
        logging.info("Loading data from parquet")
        return pd.read_parquet(parquet_pth), pd.read_parquet(base_parquet_pth)
    else:
        logging.info("Loading data from CSVs")
        books = pd.read_csv(
            "data/BX-Books.csv",
            encoding="8859",
            delimiter=";",
            on_bad_lines="skip",
            engine="pyarrow",
            dtype={"Book-Title": str},
        )

        ratings = pd.read_csv(
            "data/BX-Book-Ratings.csv",
            encoding="8859",
            delimiter=";",
            engine="pyarrow",
            dtype={"User-ID": str},
        )

        books = books.dropna()

        books["Title"] = books["Book-Title"]

        books["Book-Title"] = books["Book-Title"].str.lower()

        ratings = ratings.query("`Book-Rating` > 0")

        ratings_with_books = ratings.merge(books, on="ISBN")

        ratings_with_books = ratings_with_books.drop(
            columns=[
                "Image-URL-S",
                "Image-URL-M",
                "Image-URL-L",
            ]
        )

        clean_books_ratings = (
            ratings_with_books.groupby(["User-ID", "Book-Title"])["Book-Rating"]
            .mean()
            .reset_index()
        )

        ratings_per_book = (
            clean_books_ratings.groupby("Book-Title")["Book-Rating"]
            .count()
            .reset_index()
            .rename(columns={"Book-Rating": "Rating-Count"})
        )

        clean_books_ratings = clean_books_ratings.merge(
            ratings_per_book, on="Book-Title"
        )

        # Export to parquet
        clean_books_ratings.to_parquet("results/dataset.parquet", index=False)
        books.to_parquet("results/baseset.parquet", index=False)

        return clean_books_ratings, ratings_with_books
