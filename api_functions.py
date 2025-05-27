import pandas as pd
import requests
import os
from requests import Session
from datetime import datetime

url = "https://api.finantopay.com.br"
headers = {'accept': '*/*'}
parquet_path = "indication_payment/indication_register.parquet"


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
        register(cpf, valor, 'Pago')


def read(cpf, valor):
    with Session() as session:
        doc, payment_amount, data = puxa(cpf, session)
        if valor == payment_amount:
            pay(doc, session, valor)
        else:
            print(f"Verificar valor do CPF: {cpf}\n")
            register(doc,valor, 'Não pago, valor divergente')

def register(cpf, valor, status):
    new_data = pd.DataFrame([{
        'cpf': cpf,
        'valor': valor,
        'event_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': status
    }])

    if os.path.exists(parquet_path):
        df_existing = pd.read_parquet(parquet_path)
        df = pd.concat([df_existing, new_data], ignore_index=True)
    else:
        df = new_data
    
    df.to_parquet(parquet_path, index=False, engine='pyarrow')

