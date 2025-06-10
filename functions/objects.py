import streamlit as st
import pandas as pd
import numpy as np
from functions.api_functions import *

def upload_file():
    st.header("Indicações")
    st.write("Carregue o arquivo de indicações para processar os pagamentos.")
    uploaded_file = st.file_uploader("Escolha o arquivo", type=["xlsx"], label_visibility="collapsed")

    if uploaded_file is not None:
        base_bruta = pd.read_excel(uploaded_file, header=3)
        df = pd.DataFrame()
        df["CPF"] = base_bruta["CPF/CNPJ"].astype(str).str.rjust(11, '0')
        df["Nome"] = base_bruta["NOME COMPLETO"].astype(str)
        df["Valor"] = base_bruta["VALOR"].astype(int)


        st.session_state["df"] = df
        st.write("Resumo dos Pagamentos:")
        st.dataframe(df)
        return df
    return None

def on_click():
    df = st.session_state.get("df")
    if df is not None:
        doc, valor, nome = df["CPF"], df["Valor"], df["Nome"]
        for c, v, n in zip(doc, valor, nome):
            status = read(c, v, n)
        
        exibe_erros_streamlit()

def indication_confirm():
    if "df" in st.session_state:
    
        st.button("Confirmar Pagamentos", on_click=on_click)

    return None
