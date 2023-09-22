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
    return "test"

@app.route("/fetch")
@cache.cached()
def fetch_companies():
    result = companies_read()
    cache.set("companies", result)
    return result

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
    result = df.sort_values('precision', ascending=False).head(100).to_dict('records')
    return result

@app.route("/companies/bairro/<bairro>")
def companies_bairro(bairro):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    bairro_set = Multiset(bairro)
    
    exact = df[df["bairro"] == bairro]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['bairro'].apply(lambda row: len(bairro_set.intersection(Multiset(row))))
    result = df.sort_values('precision', ascending=False).head(100).to_dict('records')
    return result

@app.route("/companies/razao_social/<bairro>/<company>")
def companies_razao_social(bairro, company):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    df = df[df["bairro"] == unidecode(bairro.upper())]
    df = df.dropna(subset=["razao_social"])
    company = unidecode(company)
    

    exact = df[df["razao_social"] == unidecode(company.upper())]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['razao_social'].apply(lambda row: jaccard_similarity(row if row != None else "", company) + dice_coefficient(row if row != None else "", company))
    result = df.sort_values('precision', ascending=False).head(100).to_dict('records')
    return result

@app.route("/companies/nome_fantasia/<bairro>/<company>")
def companies_nome_fantasia(bairro, company):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    df = df[df["bairro"] == unidecode(bairro.upper())]
    df = df.dropna(subset=["nome_fantasia"])
    company = unidecode(company)


    exact = df[df["nome_fantasia"] == company.upper()]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['nome_fantasia'].apply(lambda row: jaccard_similarity(row if row != None else "", company) + dice_coefficient(row if row != None else "", company))
    result = df.sort_values('precision', ascending=False).head(100).to_dict('records')
    return result

@app.route("/companies/endereco/<bairro>/<tipo_logradouro>/<logradouro>")
def companies_endereco(bairro, tipo_logradouro, logradouro):
    data = cache.get("companies")
    if data == None:
        data = fetch_companies()

    df = pd.DataFrame.from_dict(data)
    df = df[df["bairro"] == unidecode(bairro.upper())]
    df = df[df["tipo_logradouro"] == tipo_logradouro.upper()]
    df = df.dropna(subset=["logradouro"])
    logradouro = unidecode(logradouro)


    exact = df[df["logradouro"] == logradouro.upper()]
    if(not exact.empty):
        return exact.to_dict('records')

    df['precision'] = df['logradouro'].apply(lambda row: jaccard_similarity(row if row != None else "", logradouro) + dice_coefficient(row if row != None else "", logradouro))
    result = df.sort_values('precision', ascending=False).head(100).to_dict('records')
    return result
  

app.run(host="localhost", port=5001)