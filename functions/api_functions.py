import pandas as pd
import requests
import os
from requests import Session
from datetime import datetime, date
from pymongo import MongoClient
import streamlit as st
import io

# Configurações
url = "https://api.finantopay.com.br"
headers = {'accept': '*/*'}
parquet_path = "indication_payment/indication_register.parquet"
MONGO_URL = "mongodb://eliton:Sudoku-%2B123@34.31.254.251:27018,34.31.254.251:27017,34.31.254.251:27019,34.31.254.251:27020,ps1ip1.ath.cx:27021/db_app?retryWrites=false&replicaSet=rs0&readPreference=secondaryPreferred&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256"
client = MongoClient(MONGO_URL, uuidRepresentation='standard')
db_app = client["db_finanto_pay"]
db_finanto_pay = db_app["customer_indication"]

df_erro = pd.DataFrame(columns=['cpf', 'nome', 'valor'])

def puxa(cpf, session):
    find_url = f'{url}/CustomerIndication/Find?documentNumber=/{cpf}'
    params = {"documentNumber": cpf}
    try:
        response = session.get(find_url, headers=headers, params=params, timeout=120)
        if response.status_code == 200:
            data = response.json()
            payment_amount = data.get("paymentAmount", 0)
            return cpf, payment_amount, data
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão: {e}")

def pay(cpf, session, valor, name):
    cpf = cpf.rjust(11, '0')
    pay_url = f'{url}/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}'
    response = session.put(pay_url, timeout=120)
    if response.status_code == 200:
        print(f"CPF {cpf} pago\n")
        register(cpf, valor, 'Pago', 'Cliente pago')
    else:
        print(f"Erro ao pagar o CPF {cpf}.")
        df_erro.loc[len(df_erro)] = [cpf, name, valor]
        register(cpf, valor, 'Não Pago', 'Erro ao pagar cliente')

def read(cpf, valor, name):
    with Session() as session:
        result = puxa(cpf, session)
        if result:
            doc, payment_amount, data = result
            if valor == payment_amount:
                pay(doc, session, valor, name)
            else:
                mongo_search(cpf, valor, name)

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

def mongo_search(document_number, commission_value, name):
    filtro = {
        "document_number": f"{document_number}",
        "commission_value": float(commission_value),
        'status_description': 'Proposta Paga'
    }

    results = db_finanto_pay.find(filtro).sort("event_datetime", 1).limit(1)

    if db_finanto_pay.count_documents(filtro) == 0:
        print(f"Nenhum documento encontrado para o CPF {document_number} com o valor de comissão {commission_value}.")
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
        register(cpf, commission_value, "Pago", "Proposta paga")
        print(f"Indicação {id} paga!")
    else:
        print(f"Erro ao pagar a indicação {id}")
        register(cpf, commission_value, "Não Pago", "Erro ao pagar proposta")
        df_erro.loc[len(df_erro)] = [cpf, name, commission_value]

def exibe_erros_streamlit():
    if not df_erro.empty:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_erro.to_excel(writer, index=False, sheet_name='Erros')
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