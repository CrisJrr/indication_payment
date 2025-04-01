import requests
from requests import Session
import pandas as pd
from api_functions import puxa, pay, read

base = pd.read_excel(r"pagamentos.xlsx")

cpf = base["CPF"]
valor = base["Valor"]

for c, v in zip(cpf, valor):
    try:
        c = str(c).rjust(11, '0')
        doc, payment = read(c, v)
        print(doc, payment)
    except:
        pass