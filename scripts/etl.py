import pandas as pd
import pathlib


def load_data() -> pd.DataFrame:
    parquet_pth = pathlib.Path("results/dataset.parquet")

    # If parquet file exists skip transformation and load and return parquet
    if parquet_pth.is_file():
        print("INFO: Reading from parquet file")
        return pd.read_parquet(parquet_pth)
    else:
        print("INFO: Parsing CSV datasets")
        # Apache Arrow bindings can handle mixed types and is faster
        # And supports MT however is inconplete.
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        # Plus it will be mandatory dep. for Pandas 3.0
        books = pd.read_csv(
            "data/BX-Books.csv",
            encoding="8859",
            delimiter=";",
            on_bad_lines="skip",
            engine="pyarrow",
            dtype={
                "Book-Title": str,
            },
            parse_dates="Year-Of-Publication",
        )

        # Dataset has missing values, dropping these rows.
        books = books.dropna()
        books = books.drop_duplicates()

        books["Year-Of-Publication"] = pd.to_datetime(
            books["Year-Of-Publication"], format="&Y"
        )

        # Need to specify the User-ID dtype becuase Apache Arrow converts it to number
        ratings = pd.read_csv(
            "data/BX-Book-Ratings.csv",
            encoding="8859",
            delimiter=";",
            engine="pyarrow",
            dtype={"User-ID": str},
        )

        # Not using inplace because it may be actually harmfull and has
        # no real benefics
        # https://github.com/pandas-dev/pandas/issues/16529
        ratings = ratings.drop_duplicates()

        # Ignoring rows with impplicit rating e.g. 0
        # Not using copy because i dont care if ratings gets modified
        # in this case
        clear_ratings = ratings.query("`Book-Rating` > 0")

        # Convert text in book-title col to lowercase so it is easier to
        # search in the there
        books["Book-Title"] = books["Book-Title"].str.lower()

        # Merge all DFs together
        merged = clear_ratings.merge(books, on="ISBN", how="left")

        # After merge there are some missing values
        # (ISBN from ratings in not in book df)
        merged = merged.dropna()

        # Drop unwanted columns
        merged = merged.drop(
            columns=["Image-URL-S", "Image-URL-M", "Image-URL-L", "Publisher"],
        )

        number_of_votes = (
            merged.groupby("Book-Title")["Book-Rating"]
            .agg("count")
            .reset_index()
        )

        number_of_votes = number_of_votes.rename(
            columns={"Book-Rating": "Votes-Number"}
        )

        merged = merged.merge(
            number_of_votes, on="Book-Title", how="left"
        ).reset_index(drop=True)

        # Save as Parquet file so I dont have to do this every time
        # (In case dataset did not change ofc.)

        merged.to_parquet("results/dataset.parquet")

        return merged
