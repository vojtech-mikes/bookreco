import pandas as pd


def create_recommendation(user_input: str, data: pd.DataFrame):

    # Select only rows that contains rating of user selected book
    user_books = data.query("`Book-Title` == @user_input")

    # Group by USER to eliminate possible duplicate rating and save mean
    # values of the ratings
    user_books = user_books.groupby("User-ID")["Book-Rating"].mean()
    user_books = user_books.reset_index()

    # Set treshold to the number of unique ratings. This is base lenght of
    # our dataset A.
    TRESHOLD = len(user_books["Book-Rating"])

    # Search for all books that have same number of votes as treshold
    books_to_evaluate = data.query("`Votes-Number` >= @TRESHOLD")

    # Also remove duplicates by groupb
    books_to_evaluate = books_to_evaluate.groupby(["User-ID", "Book-Title"])[
        "Book-Rating"
    ].mean()
    books_to_evaluate = books_to_evaluate.reset_index()

    books_to_evaluate.info()

    # Create pivot
    books_pivot = books_to_evaluate.pivot(
        index="User-ID", columns="Book-Title", values="Book-Rating"
    )
