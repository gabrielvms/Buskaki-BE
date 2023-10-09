import os
from flask import Flask, redirect, url_for
from flask_cors import CORS
from flask_caching import Cache
from multiset import Multiset
from unidecode import unidecode
import pandas as pd
import sys
sys.path.append('api/helpers')
from db_functions import *
from functions import *


config = {

    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 0
}

app = Flask(__name__, static_url_path="")
app.config.from_mapping(config)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
cache = Cache(app)

@app.route("/")
def default():
    return redirect(url_for("companies", page=1)) 

@app.route("/fetch")
@cache.cached()
def fetch_companies():
    result = pd.read_json('empresas.json.gz', compression='gzip')
    cache.set("companies", result.to_dict())
    return result

@app.route("/cnpjs")
def cnpjs():
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    return df["cnpj"].to_list()

@app.route("/companies/advanced/<value>")
def advanced(value):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    value = unidecode(value)
    df = pd.DataFrame.from_dict(data)
    result = df[df["nome_fantasia"].str.contains(str(value).upper())]
    result = pd.concat([result, df[df["razao_social"].str.contains(str(value).upper())]])
    result = pd.concat([result, df[df["logradouro"].str.contains(str(value).upper())]])
    result = pd.concat([result, df[df["bairro"].str.contains(str(value).upper())]])
    return result.head(1000).to_dict('records')

@app.route("/companies/<page>")
def companies(page):
    page = int(page)
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    start = 50*(page-1)
    end = 50*page
    dfRange = df.iloc[start:end]
    return dfRange.to_dict("records")


@app.route("/companies/cnpj/<cnpj>")
def companies_cnpj(cnpj):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    cnpj_set = Multiset(cnpj)
    
    exact = df[df["cnpj"] == cnpj]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['cnpj'].apply(lambda row: len(cnpj_set.intersection(Multiset(row))))
    result = df.sort_values('precision', ascending=False).head(1000).to_dict('records')
    return result

@app.route("/companies/bairro/<bairro>")
def companies_bairro(bairro):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    
    exact = df[df["bairro"] == bairro.upper()]
    return exact.to_dict('records')

@app.route("/companies/razao_social/<company>")
def companies_razao_social(company):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    # df = df[df["bairro"] == unidecode(bairro.upper())]
    df = df.dropna(subset=["razao_social"])
    company = unidecode(company)
    

    exact = df[df["razao_social"] == unidecode(company.upper())]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['razao_social'].apply(lambda row: jaccard_similarity(row if row != None else "", company) + dice_coefficient(row if row != None else "", company))
    result = df.sort_values('precision', ascending=False).head(1000).to_dict('records')
    return result

@app.route("/companies/nome_fantasia/<company>")
def companies_nome_fantasia(company):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    # df = df[df["bairro"] == unidecode(bairro.upper())]
    df = df.dropna(subset=["nome_fantasia"])
    company = unidecode(company)


    exact = df[df["nome_fantasia"] == company.upper()]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['nome_fantasia'].apply(lambda row: jaccard_similarity(row if row != None else "", company) + dice_coefficient(row if row != None else "", company))
    result = df.sort_values('precision', ascending=False).head(1000).to_dict('records')
    return result

@app.route("/companies/endereco/<logradouro>")
def companies_endereco(logradouro):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    # df = df[df["bairro"] == unidecode(bairro.upper())]
    # df = df[df["tipo_logradouro"] == tipo_logradouro.upper()]
    df = df.dropna(subset=["logradouro"])
    logradouro = unidecode(logradouro)


    exact = df[df["logradouro"] == logradouro.upper()]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['logradouro'].apply(lambda row: jaccard_similarity(row if row != None else "", logradouro) + dice_coefficient(row if row != None else "", logradouro))
    result = df.sort_values('precision', ascending=False).head(1000).to_dict('records')
    return result

app.run(host="localhost", port=5001)