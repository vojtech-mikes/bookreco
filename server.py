from flask import Flask, render_template, request
from scripts.data_service import create_response

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/rcm", methods=["POST"])
def rcm():
    search_term = str(request.form["search_term"]).lower()
    search_typ = str(request.form["search_typ"]).lower()

    result = create_response(search_term, search_typ)

    return render_template("result_table.html", table=result)


@app.errorhandler(ValueError)
def server_error(e):
    return render_template("server_error.html")
