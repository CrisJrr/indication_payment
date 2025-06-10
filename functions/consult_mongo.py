from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import httpx
import uuid
from requests import Session
from api_functions import register

url = "https://api.finantopay.com.br"
MONGO_URL = "mongodb://eliton:Sudoku-%2B123@34.31.254.251:27018,34.31.254.251:27017,34.31.254.251:27019,34.31.254.251:27020,ps1ip1.ath.cx:27021/db_app?retryWrites=false&replicaSet=rs0&readPreference=secondaryPreferred&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256"
client = MongoClient(MONGO_URL, uuidRepresentation='standard')
db_app  = client["db_finanto_pay"]
db_finanto_pay = db_app["customer_indication"]


def mongo_search(document_number, commission_value):

    filtro = {
        "document_number": f"{document_number}",
        "commission_value": float(commission_value),
        'status_description': 'Proposta Paga'
    }

    results = db_finanto_pay.find(filtro).sort("event_datetime", -1).limit(1)

    if db_finanto_pay.count_documents(filtro) == 0:
        print(f"Nenhum documento encontrado para o CPF {document_number} com o valor de comissão {commission_value}.")
    else:
        for doc in results:
            id = doc.get("_id")
            cpf = doc.get("document_number")
            print(f"ID: {id}")
    
    indication_id(id, cpf, commission_value, Session())
    return id, cpf, commission_value

def indication_id(id, cpf, commission_value, session):
    indication_pay = f"{url}/CustomerIndication/CommisionDisbursedProposl/{id}"

    response = session.put(indication_pay, timeout= 120)
    if response.status_code == 200:
        register(cpf, commission_value, "Pago")
        print(f"Indicação {id} paga!")
    else:
        register(cpf, commission_value, "Proposta não paga")
