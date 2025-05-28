import pandas as pd
import requests
import os
from requests import Session
from datetime import datetime
from pymongo import MongoClient

url = "https://api.finantopay.com.br"
headers = {'accept': '*/*'}
parquet_path = "indication_payment/indication_register.parquet"
MONGO_URL = "mongodb://eliton:Sudoku-%2B123@34.31.254.251:27018,34.31.254.251:27017,34.31.254.251:27019,34.31.254.251:27020,ps1ip1.ath.cx:27021/db_app?retryWrites=false&replicaSet=rs0&readPreference=secondaryPreferred&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256"
client = MongoClient(MONGO_URL, uuidRepresentation='standard')
db_app  = client["db_finanto_pay"]
db_finanto_pay = db_app["customer_indication"]


def puxa(cpf, session):
        find_url = f'{url}/CustomerIndication/Find?documentNumber=/{cpf}'
        params = {"documentNumber": cpf}
        try:
            response = session.get(find_url, headers = headers, params=params, timeout=120)
            if response.status_code == 200:
                data = response.json()
                payment_amount = data.get("paymentAmount", 0)

                return (cpf, payment_amount, data)
            else:
                print("Erro")
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")

def pay(cpf, session, valor):
    cpf = cpf.rjust(11, '0')
    pay_url = f'{url}/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}'
    response = session.put(pay_url, timeout= 120)
    if response.status_code == 200:
        retorno = response.json()
        print(f"CPF {cpf} pago\n")
        register(cpf, valor, 'Pago', 'Cliente pago')

def read(cpf, valor):
    with Session() as session:
        doc, payment_amount, data = puxa(cpf, session)
        if valor == payment_amount:
            pay(doc, session, valor)
        else:
            mongo_search(cpf, valor)

def register(cpf, valor, status, description):
    new_data = pd.DataFrame([{
        'cpf': cpf,
        'valor': valor,
        'event_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': status,
        'description': description
    }])

    if os.path.exists(parquet_path):
        df_existing = pd.read_parquet(parquet_path)
        df = pd.concat([df_existing, new_data], ignore_index=True)
    else:
        df = new_data

    df.to_parquet(parquet_path, index=False, engine='pyarrow')

def mongo_search(document_number, commission_value):

    filtro = {
        "document_number": f"{document_number}",
        "commission_value": float(commission_value),
        'status_description': 'Proposta Paga'
    }

    results = db_finanto_pay.find(filtro).sort("event_datetime", 1).limit(1)

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
        register(cpf, commission_value, "Pago", "Proposta paga")
        print(f"Indicação {id} paga!")
    else:
        print(f"Erro ao pagar a indicação {id}.")
        register(cpf, commission_value, "Não Pago", "Erro ao pagar proposta")