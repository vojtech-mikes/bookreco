import pandas as pd
import logging


def calculate_recommendations(
    books: pd.DataFrame, searched: str
) -> pd.DataFrame:
    logging.info("Calculating recommendations")
    user_book_raters = books.query("`Book-Title` == @searched")

    users_uniq = user_book_raters["User-ID"].unique()

    TRESHOLD = 50

    raters_other_books = books.query(
        "`User-ID` in @users_uniq and `Rating-Count` > @TRESHOLD and `Book-Title` != @searched"
    )

    raters_other_books_uniq = raters_other_books["User-ID"].unique()

    true_unique_readers = set(users_uniq).intersection(raters_other_books_uniq)

    user_book_raters = user_book_raters.query(
        "`User-ID` in @true_unique_readers"
    )

    raters_other_books = (
        raters_other_books.groupby(["User-ID", "Book-Title"])["Book-Rating"]
        .mean()
        .reset_index()
    )

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
