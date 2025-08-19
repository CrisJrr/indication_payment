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
import itertools
from bson.binary import Binary

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
        self.mongo_cris = os.getenv("MONGO_CRIS")
        self.mongo_qa = os.getenv("MONGO_QA")
        self.mongo_prod = os.getenv("MONGO_PROD")
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

        uri = MongoClient(self.mongo_cris)
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

        print(f"[INSERT]Documento criado para o CPF: {doc}\n")

    def pay(self, cpf, session, commission_value, name, user):
        cpf = cpf.rjust(11, "0")
        pay_url = self.finpay + f"/CustomerIndication/CommissionDisbursedToClient?documentNumber={cpf}"
        response = session.put(pay_url, timeout=120)
        if response.status_code == 200:
            print(f"[PAY]CPF {cpf} pago\n")
            self.insert(user, cpf, name, commission_value, "Pago", "Cliente pago")
            return []
        else:
            print(f"[PAY]Erro ao pagar o CPF {cpf}.")
            self.insert(user, cpf, name, commission_value, "Não Pago", "Erro ao pagar cliente")
            error_details = [{"CPF": cpf, "ID com Erro": "N/A - Pagamento Direto"}]

            return error_details

    def read(self, cpf, commission_value, name, user, pay_data):
        try:
            with Session() as session:
                result = self.puxa(cpf, session)

                if not result:
                    return [{
                        "CPF": cpf, "Nome": name, "Valor": commission_value,
                        "Motivo da Falha": "CPF não encontrado na consulta inicial (API Puxa)."
                    }]

                doc, payment_amount, data = result

                if commission_value == payment_amount:
                    return self.pay(doc, session, commission_value, name, user)

                else:
                    return self.mongo_search(doc, commission_value, name, user, pay_data)

        except Exception as e:
            print(f"[ERRO GERAL] Um erro inesperado ocorreu ao processar o CPF {cpf}: {e}")
            return [{
                "CPF": cpf, "Nome": name, "Valor": commission_value,
                "Motivo da Falha": f"Erro inesperado durante o processamento: {e}"
            }]

    def mongo_search(self, document_number, commission_value, name, user, pay_data):
        
        finanto_client = MongoClient(self.mongo_prod)
        db_app = finanto_client["db_finanto_pay"]
        db_finanto_pay = db_app["customer_indication"]

        filtro = {
            "document_number": f"{document_number}",
            "status_description": "Proposta Paga",
            "event_datetime": {
                "$lt": f"{pay_data}T00:00:00.000+0000"
            }
        }

        results = list(db_finanto_pay.find(filtro).sort("event_datetime", 1))

        if not results:
            print(f"[MONGO]Nenhum documento encontrado para o CPF {document_number}.")
            self.insert(user, document_number, name, commission_value, "Não Pago", "Nenhum documento encontrado")
            return [{"CPF": document_number, "Nome": name, "Valor": commission_value, "Motivo da Falha": "Nenhum documento de indicação paga encontrado no MongoDB."}]
 
        comiss_total = round(float(commission_value), 2)
        docs_to_pay = None

        for i in range(1, len(results) +1):
            for combo in itertools.combinations(results, i):
                current_sum = sum(round(doc.get("commission_value", 0), 2) for doc in combo)
                if current_sum == comiss_total:
                    docs_to_pay = list(combo)
                    break
            if docs_to_pay:
                break
        
        if docs_to_pay:
            ids_to_pay = []
            for doc in docs_to_pay:
                doc_id = doc.get("_id")
                if isinstance(doc_id, Binary):
                    ids_to_pay.append(str(uuid.UUID(bytes=doc_id)))
                else:
                    ids_to_pay.append(str(doc_id))
            
            cpf = docs_to_pay[0].get("document_number")
            return self.indication_id(ids_to_pay, cpf, commission_value, Session(), name, user)
        else:
            total_found = sum(doc.get("commission_value", 0) for doc in results)
            print(f"[MONGO] Nenhuma combinação de documentos resultou no valor esperado de R${comiss_total} para o CPF {document_number}. Valor total encontrado: R${total_found}")
            self.insert(user, document_number, name, commission_value, "Não Pago", "Valor de comissão divergente (nenhuma combinação encontrada)")
            error_reason = f"Nenhuma combinação somou R${comiss_total}. (Valor total pendente: R${total_found})"
            return [{"CPF": document_number, "Nome": name, "Valor": commission_value, "Motivo da Falha": error_reason}]

    def indication_id(self, ids, cpf, commission_value, session, name, user):
        indication_url = self.finpay + f"/CustomerIndication/CommissionDisbursedProposal"
        headers = {"Content-Type": "application/json"}

        succes_ids = []
        failed_ids = {}
        report_data_failed = []

        for id in ids:
            try:
                payload = [id]
                response = session.put(indication_url, json=payload, headers=headers, timeout=120)

                if response.status_code == 200:
                    # self.insert(user, cpf, name, commission_value, "Pago", "Pago via ID")
                    print(f"[ID]Indicação {id} paga!")
                    succes_ids.append(id)
                else:
                    print(f"[ID]Erro ao pagar a indicação {id}: {response.text}")
                    reason = response.text
                    failed_ids[id] = reason

            except requests.exceptions.RequestException as e:
                reason = "Erro de conexão"
                print(f"[ID] Erro ao pagar a indicação {id}: {e}")
                failed_ids[id] = reason

        total_ids = len(ids)
        succes_count = len(succes_ids)

        if not failed_ids:
            # SUCESSO TOTAL: Todos os IDs foram pagos.
            self.insert(user, cpf, name, commission_value, "Pago", f"Todos os {len(ids)} IDs pagos com sucesso.")
        
        elif not succes_ids:
            # FALHA TOTAL: Nenhum ID foi pago.
            details = "; ".join([f"ID {id}: {reason}" for id, reason in failed_ids.items()])
            self.insert(user, cpf, name, commission_value, "Não Pago", f"Falha no pagamento de todos os IDs. Detalhes: {details}")
        
        else:
            details = "; ".join([f"ID {id}: {reason}" for id, reason in failed_ids.items()])
            # Cria dois registros claros para auditoria
            self.insert(user, cpf, name, commission_value, "Pago", f"{len(succes_ids)} de {len(ids)} IDs pagos com sucesso.")
            self.insert(user, cpf, name, 0, "Não Pago", f"Falha no pagamento de {len(failed_ids)} IDs. Detalhes: {details}")

        # 3. --- GERA O RELATÓRIO DE FALHAS CORRETAMENTE ---
        for failed_id, reason in failed_ids.items():
            report_data_failed.append({
                "CPF": cpf,
                "ID da Indicação": failed_id,
                "Motivo da Falha": reason
            })
            
        return report_data_failed