from numpy.random.mtrand import sample
import pandas as pd


def define_occurence_treshold(df: pd.DataFrame, uniq_users) -> pd.DataFrame:
    # Treshold should be the maximum number that produces 40 results at minimum
    # Otherwise correlation calculation may be very inaccured

    data_to_process = df.query("`User-ID` in @uniq_users").copy()

    treshold = data_to_process["Votes-Number"].max(axis=0)

    count = 0
    data = pd.DataFrame()

    # NOTE: This may or may not be very expensive, depends on initial dataset size
    # Count value is determined by following: Bonett and Wright (2000),
    # Sample size requirements for estimating ¨Pearson, Kendall and Spearman correlations,
    # Psychometrika, 65(1), 23-2
    # And
    # https://www.researchgate.net/post/What-is-the-minimum-sample-size-to-run-Pearsons-R
    sufficient_sample = 30

    while count < sufficient_sample:
        data = data_to_process.query("`Votes-Number` >= @treshold")
        count = data.shape[0]

        if count >= sufficient_sample:
            break

        treshold = treshold - 1

    print(f"Found optimal treshold of: {treshold}")

    return data


def create_recommendation(user_input: dict, data: pd.DataFrame):
    # Calculate number of votes per user

    # Find all users that voted for searched book, so then the
    # dataset is smaller
    book_ratings = data.query("`Book-Title` == @user_input").copy()

    book_voters = book_ratings["User-ID"].unique()

    book_for_corr = define_occurence_treshold(data, book_voters)

    print(book_for_corr.info())
