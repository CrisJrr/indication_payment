import pandas as pd
import requests
from requests import Session

url = "https://api.finantopay.com.br"
headers = {'accept': '*/*'}
# print(cli)

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


def pay(cpf, session):
    cpf = cpf.rjust(11, '0')
    pay_url = f'{url}/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}'
    response = session.put(pay_url, timeout= 120)
    if response.status_code == 200:
        retorno = response.json()
        print(f"CPF: {cpf} feito\n")


def read(cpf, valor):
    with Session() as session:
        doc, payment_amount, data = puxa(cpf, session)
        print(f"\nCPF: {doc}\nPayment Amount: {payment_amount}")
        if valor == payment_amount:
            pay(doc, session)
        else:
            print(f"Valor incorreto: \nOriginal: {payment_amount}\nRegistrado: {valor}\n")
