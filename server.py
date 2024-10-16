from flask import Flask, make_response, render_template, request
from scripts.data_service import create_response

app = Flask(__name__)


@app.route("/")
def home():
    resp = make_response(render_template("index.html"))

    return resp


@app.route("/rcm", methods=["POST"])
def rcm():
    search_term = str(request.form["search_term"]).lower()

    result = create_response(search_term)
    resp = make_response(render_template("result_table.html", table=result))

    return resp
