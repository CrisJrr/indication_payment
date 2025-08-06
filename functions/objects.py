import streamlit as st
import pandas as pd
from functions.new_functions import *

fb = Fatban()

def upload_file():
    st.header("Indicações")
    st.write("Carregue o arquivo de indicações para processar os pagamentos.")
    login = st.text_input(
        "Digite seu login:"
        , placeholder="Seu login aqui")
    uploaded_file = st.file_uploader("Escolha o arquivo", type=["xlsx"], label_visibility="collapsed")

    if uploaded_file is not None:
        # pay_data = re.match(r"^(\d{2}\.\d{2}\.\d{4})", uploaded_file.name)
        # pay_data = re.sub(r"\.", "-", pay_data)
        # pay_data = datetime.strptime(pay_data, '%d-%m-%Y').strftime('%Y-%m-%d')

        match = re.match(r"^(\d{2}\.\d{2}\.\d{4})", uploaded_file.name)
        if match:
            pay_data = match.group(1)  # extrai a string da data
            pay_data = re.sub(r"\.", "-", pay_data)  # agora sim: substitui os pontos por hífens
            pay_data = datetime.strptime(pay_data, '%d-%m-%Y').strftime('%Y-%m-%d')

        base_bruta = pd.read_excel(uploaded_file, header=3)
        df = pd.DataFrame()
        df["CPF"] = base_bruta["CPF/CNPJ"].astype(str).str.rjust(11, '0')
        df["Nome"] = base_bruta["NOME COMPLETO"].astype(str)
        df["Valor"] = base_bruta["VALOR"].astype(int)

        st.session_state["df"] = df
        st.session_state["login"] = login
        st.session_state["pay_data"] = pay_data
        st.write("Resumo dos Pagamentos:")
        st.dataframe(df)

        return df, pay_data
    return None

def on_click(user):
    df = st.session_state.get("df")
    pay_data = st.session_state.get("pay_data")

    user = st.session_state.get["login"]

    if df is not None:
        doc, valor, nome = df["CPF"], df["Valor"], df["Nome"]
        for c, v, n in zip(doc, valor, nome):
            status = fb.read(c, v, n, user, pay_data)

def indication_confirm():
    if "df" in st.session_state:

        st.button("Confirmar Pagamentos", on_click=on_click)

    return None
