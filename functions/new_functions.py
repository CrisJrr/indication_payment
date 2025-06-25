from datetime import datetime, timedelta
import uuid
from dateutil import parser
import requests
from requests import Session
import httpx
import os
from dotenv import load_dotenv
import pandas as pd
import time
from pymongo import MongoClient
import re

class Fatban:
    def __init__(self):
        load_dotenv()
        self.client = httpx.Client()
        self.user_id = os.getenv("USER_ID_LOGIN_FATBAN")
        self.api_key = os.getenv("API_KEY_FINANTO")
        self.endpoint = os.getenv("FATBAN_ENDPOINT")
        self.finpay = os.getenv("PAY_ENDPOINT")
        self.aws_link = os.getenv("AWS_LINK")
        self.auth_endpoint = os.getenv("AUTH_FATBAN_ENDPOINT")
        self.mongo = os.getenv("MONGO_URI")
        self.token = self.login_fatban()

    def login_fatban(self) -> str | None:

        print(f"Realizando login no CRM!")

        url = f"{self.auth_endpoint}/token/{self.user_id}"
        _body = {
                    "x-api-key": self.api_key
                }
        res = self.client.post(url, headers=_body)
        if res.status_code == 200:
            return res.json()['access_token']
        elif res.status_code == 401:
            if res.json()["messages"][0]["text"] == "Usuário ou senha inválidos":
                print("FATBAN: Usuário ou senha inválidos")
                return None
        return self.login_fatban()

    def login_fatban_api(self, login: str, password: str) -> str:

        print(f"Conectando a API!")

        data = {'accessKey': login, 'password': password}
        _url = self.endpoint + '/login'
        res = self.client.post(_url, json=data)
        if res.status_code == 200:
            res_json = res.json()
            token = res_json['token']
            self.token = token
            return token
        elif res.status_code == 401:
            raise Exception("Login do Fatban está inválido")
        else:
            return self.login_fatban_api(login, password)

    def puxa(self, cpf, session):
        find_url = self.finpay + f"/CustomerIndication/Find?documentNumber=/{cpf}"
        params = {"documentNumber": cpf}
        headers = {"accept": "*/*"}
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

        _url = self.endpoint + '/users/accessKey/' + accessKey
        _auth = {"authorization" : "Bearer " + self.token}
        res = self.client.get(_url, headers=_auth, timeout=30)
        if res.status_code == 200:
            data = res.json()
            id = data['userId']
            print(f"Id encontrado para {accessKey}")
            return id
        elif res.status_code == 401:
            return self.get_user_id(accessKey)
        
    def insert(self, user, doc, name, commission_value, status, description):

        uri = MongoClient(self.mongo)
        db_ind = uri["finanto_indication"]
        indication_collection = db_ind["indication_payment"]

        indication_collection.insert_one(
            {
                "_id": str(uuid.uuid4()),
                "cpf_indicador": doc,
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

    def pay(self, cpf, session, commission_value, name, user):
        cpf = cpf.rjust(11, "0")
        pay_url = self.finpay + f"/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}"
        response = session.put(pay_url, timeout=120)
        if response.status_code == 200:
            print(f"CPF {cpf} pago\n")
            self.insert(user, cpf, name, commission_value, "Pago", "Cliente pago")
        else:
            print(f"Erro ao pagar o CPF {cpf}.")
            self.insert(user, cpf, name, commission_value, "Não Pago", "Erro ao pagar cliente")

    def read(self, cpf, commission_value, name, user):
        with Session() as session:
            result = self.puxa(cpf, session)
            if result:
                doc, payment_amount, data = result
                if commission_value == payment_amount:
                    self.pay(doc, session, commission_value, name, user)
                else:
                    self.mongo_search(doc, commission_value, name, user)

    def mongo_search(self, document_number, commission_value, name, user):

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
            self.insert(user, document_number, name, commission_value, "Não Pago", "Nenhum documento encontrado")
        else:
            for doc in results:
                id = str(doc.get("_id"))
                cpf = doc.get("document_number")
                self.indication_id(id, cpf, commission_value, Session(), name, user)

    def indication_id(self, id, cpf, commission_value, session, name, user):
        indication_url = self.pay + f"/CustomerIndication/CommissionDisbursedProposal"
        payload = [id]
        headers = {"Content-Type": "application/json"}

        response = session.put(indication_url, json=payload, headers=headers, timeout=120)
        print(response)
        if response.status_code == 200:
            self.insert(user, cpf, name, commission_value, "Pago", "Pago via ID")
            print(f"Indicação {id} paga!")
        else:
            print(f"Erro ao pagar a indicação {id}")
            self.insert(user, cpf, name, commission_value, "Não Pago", "Erro ao pagar proposta")
