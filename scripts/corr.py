import pandas as pd


def create_recommendation(user_input: dict, data: pd.DataFrame):
    # Get searched book with ratings
    print(data.head())

    print(user_input)
