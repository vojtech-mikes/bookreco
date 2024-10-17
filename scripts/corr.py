import pandas as pd
import logging
from scripts.utils import prepare_data


def calculate_recommendations_corr(
    books: pd.DataFrame, searched: str
) -> pd.DataFrame:
    logging.info("Calculating recommendations")
    user_book_raters, raters_other_books = prepare_data(books, searched)

    pivot_books = raters_other_books.pivot(
        index="User-ID", columns="Book-Title", values="Book-Rating"
    )

    pivot_user_book = user_book_raters.pivot(
        index="User-ID", columns="Book-Title", values="Book-Rating"
    )

    pivot_books = pivot_books.fillna(0)

    user_book = pivot_user_book[searched]

    corrs = []
    book_names = []

    for col in pivot_books.columns.values:

        book = pivot_books[col]

        if isinstance(book, pd.Series) and isinstance(user_book, pd.Series):
            if book.nunique() > 1:
                corrs.append(book.corr(user_book))
                book_names.append(col)

    result = pd.DataFrame({"Book-Title": book_names, "Correlation": corrs})

    result = result.sort_values(by="Correlation", ascending=False).head(10)

    return result
