from numpy import sort
import pandas as pd
import logging
from sklearn.metrics.pairwise import cosine_similarity
from scripts.utils import prepare_data


def calculate_recommendations_sim(books: pd.DataFrame, searched: str):
    logging.info("Calculating recommendations")

    user_book_raters, raters_other_books = prepare_data(books, searched)

    # Pivoting these like this because scikit cosine simolirity methods
    # expects data like shape (n_samples_X, n_features)
    pivot_books = raters_other_books.pivot(
        index="Book-Title", columns="User-ID", values="Book-Rating"
    )

    pivot_books = pivot_books.fillna(0)

    pivot_user_book = user_book_raters.pivot(
        index="Book-Title", columns="User-ID", values="Book-Rating"
    )

    results = cosine_similarity(pivot_user_book, pivot_books)[0]

    pivot_books["Similarity"] = results

    pivot_books = pivot_books.reset_index()

    result_df = (
        pivot_books[["Book-Title", "Similarity"]]
        .reset_index()
        .sort_values(by="Similarity", ascending=False)
        .head(10)
    )

    return result_df
