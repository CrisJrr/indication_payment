import requests
from requests import Session
import pandas as pd
from api_functions import puxa, pay, read

base = pd.read_excel(r"pagamentos.xlsx")
base_csv = pd.read_csv(r"pagamentos.csv")

cpf = base_csv["cpf"]
valor = base_csv["valor"]

for c, v in zip(cpf, valor):
    try:
        c = str(c).rjust(11, '0')
        doc, payment = read(c, v)
        print(doc, payment)
    except:
        pass