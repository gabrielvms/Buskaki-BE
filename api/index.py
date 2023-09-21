import os
from flask import Flask, redirect, url_for
from flask_cors import CORS
from flask_caching import Cache
from multiset import Multiset
from unidecode import unidecode
import functions as fc
import pandas as pd

config = {

    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 86400
}

app = Flask(__name__, static_url_path="")
app.config.from_mapping(config)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
cache = Cache(app)

@app.route("/")
def default():
    return os.environ.get('POSTGRES_URL')
