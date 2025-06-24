import pandas as pd
import requests
import os
from requests import Session
from datetime import datetime, date
from pymongo import MongoClient
from dotenv import load_dotenv
import httpx
import streamlit as st
import io
import uuid

# Configurações
url = "https://api.finantopay.com.br"
headers = {"accept": "*/*"}
parquet_path = "indication_payment/indication_register.parquet"
client = httpx.Client()

df_erro = pd.DataFrame(columns=["cpf", "nome", "commission_value"])

def puxa(cpf, session):
    find_url = f"{url}/CustomerIndication/Find?documentNumber=/{cpf}"
    params = {"documentNumber": cpf}
    try:
        response = session.get(find_url, headers=headers, params=params, timeout=120)
        if response.status_code == 200:
            data = response.json()
            payment_amount = data.get("paymentAmount", 0)
            return cpf, payment_amount, data
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão: {e}")

def get_user_id(self, accessKey:str):

    print(f"Buscando Id para usuário: {accessKey}")

    _url = "https://api.finanto.io/v2" + '/users/accessKey/' + accessKey
    _auth = {"authorization" : "Bearer " + self.token}
    res = client.get(_url, headers=_auth, timeout=30)
    if res.status_code == 200:
        data = res.json()
        id = data['userId']
        print(f"Id encontrado para {accessKey}")
        return id
    elif res.status_code == 401:
        return self.get_user_id(accessKey)

def pay(cpf, session, commission_value, name):
    cpf = cpf.rjust(11, "0")
    pay_url = f"{url}/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}"
    response = session.put(pay_url, timeout=120)
    if response.status_code == 200:
        print(f"CPF {cpf} pago\n")
        insert(user, cpf, name, commission_value, "Pago", "Cliente pago")
    else:
        print(f"Erro ao pagar o CPF {cpf}.")
        df_erro.loc[len(df_erro)] = [cpf, name, commission_value]
        insert(user, cpf, name, commission_value, "Não Pago", "Erro ao pagar cliente")

def read(cpf, commission_value, name):
    with Session() as session:
        result = puxa(cpf, session)
        if result:
            doc, payment_amount, data = result
            if commission_value == payment_amount:
                pay(doc, session, commission_value, name)
            else:
                mongo_search(doc, commission_value, name)

def mongo_search(document_number, commission_value, name):

    mongo_finanto = "mongodb://eliton:Sudoku-%2B123@34.31.254.251:27018,34.31.254.251:27017,34.31.254.251:27019,34.31.254.251:27020,ps1ip1.ath.cx:27021/db_app?retryWrites=false&replicaSet=rs0&readPreference=secondaryPreferred&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256"
    finanto_client = MongoClient(mongo_finanto, uuidRepresentation="standard")
    db_app = finanto_client["db_finanto_pay"]
    db_finanto_pay = db_app["customer_indication"]

    filtro = {
        "document_number": f"{document_number}",
        "commission_value": float(commission_value),
        "status_description": "Proposta Paga"
    }

    results = db_finanto_pay.find(filtro).sort("event_datetime", 1).limit(1)

    if db_finanto_pay.count_documents(filtro) == 0:
        print(f"Nenhum documento encontrado para o CPF {document_number} com o commission_value de comissão {commission_value}.")
        insert(user, cpf, name, commission_value, "Não Pago", "Nenhum documento encontrado")
        df_erro.loc[len(df_erro)] = [document_number, name, commission_value]
    else:
        for doc in results:
            id = str(doc.get("_id"))
            cpf = doc.get("document_number")
            indication_id(id, cpf, commission_value, Session(), name)

def indication_id(id, cpf, commission_value, session, name):
    indication_url = f"{url}/CustomerIndication/CommissionDisbursedProposal"
    payload = [id]
    headers = {"Content-Type": "application/json"}

    response = session.put(indication_url, json=payload, headers=headers, timeout=120)
    print(response)
    if response.status_code == 200:
        insert(user, cpf, name, commission_value, "Pago", "Pago via ID")
        print(f"Indicação {id} paga!")
    else:
        print(f"Erro ao pagar a indicação {id}")
        insert(user, cpf, name, commission_value, "Não Pago", "Erro ao pagar proposta")
        df_erro.loc[len(df_erro)] = [cpf, name, commission_value]

def exibe_erros_streamlit():
    if not df_erro.empty:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_erro.to_excel(writer, index=False, sheet_name="Erros")
        output.seek(0)

        st.warning("Erros encontrados durante o processamento!")
        st.download_button(
            label="📥 Baixar lista de erros",
            data=output,
            file_name=f"Erros_Pagamento_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_erros_final"
        )

        df_erro.drop(df_erro.index, inplace=True)



mongo_cris= "mongodb+srv://cris:B3RasYUGMroTFYa9@consult-history.tbbllne.mongodb.net/"
uri = MongoClient(mongo_cris, uuidRepresentation="standard")
db_ind = uri["finanto_indication"]
indication_collection = db_ind["indication_payment"]

def insert(user, doc, name, commission_value, status, description):

    indication_collection.insert_one(
        {
            "_id": str(uuid.uuid4()),
            "cpf_indicado": doc,
            "name": name,
            "user": user,
            "commission_value": commission_value,
            "status_description": {
                "status": status,
                "description": description
            },
            "creation_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        )

    print(f"Documento criado para o CPF: {doc}")

def update(self, user, key, status_front, erro, event_datetime):

    id = indication_collection.find_one(
        {   
            "user": user,
            "key_simulation": key
        }
    )

    if id is None:
        print(f"Nenhum documento encontrado com a chave: {key}")
        return
    else:
        indication_collection.update_one(
            {
                "key_simulation": key
            }
            ,{
                "$set": {
                    "status_description.status": status_front,
                    "status_description.description": erro,
                    "event_datetime": event_datetime
                }

            },
            upsert=True
            )
    print(f"Documento atualizado com o ID: {key}")