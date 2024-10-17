import pandas as pd


def prepare_data(data: pd.DataFrame, searched: str):
    user_book_raters = data.query("`Book-Title` == @searched")

    users_uniq = user_book_raters["User-ID"].unique()

    TRESHOLD = 50

    raters_other_books = data.query(
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

    return user_book_raters, raters_other_books
