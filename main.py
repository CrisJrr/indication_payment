import requests
from requests import Session
import pandas as pd
from api_functions import puxa, pay, read
import os


def listar_arquivos_xlsx(pasta_raiz):
    arquivos_xlsx = None
    
    for raiz, _, arquivos in os.walk(pasta_raiz):
        for arquivo in arquivos:
            if arquivo.lower().endswith('.xlsx'):
                caminho_completo = os.path.join(raiz, arquivo)
                arquivos_xlsx = caminho_completo
    
    return arquivos_xlsx

path = listar_arquivos_xlsx("/home/crisbrandt/Documentos/github/cris/indication/indication_payment/")

base = pd.read_excel(path, header=3)

cpf = base["CPF/CNPJ"]
valor = base["VALOR"]

for c, v in zip(cpf, valor):
    try:
        c = str(c).rjust(11, '0')
        doc, payment = read(c, v)
    except:
        pass