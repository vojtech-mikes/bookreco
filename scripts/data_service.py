import pandas as pd
from etl import load_data
from corr import create_recommendation


def recommend(user_input):
    data = load_data()

    return create_recommendation(user_input, data)


# NOTE: Smazat, tohle je jen pro dev

dummy = "the hobbit"

recommend(dummy)
