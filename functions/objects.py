import streamlit as st
import pandas as pd
from functions.new_functions import *
import io

fb = Fatban()

def salvar_login():
    st.session_state["login"] = st.session_state["login_input"]

def upload_file():
    st.header("Indicações")
    st.write("Carregue o arquivo de indicações para processar os pagamentos.")
    st.text_input(
        "Digite seu login:",
        placeholder="Seu login aqui",
        key="login_input",
        on_change=salvar_login
    )
    uploaded_file = st.file_uploader("Escolha o arquivo", type=["xlsx"], label_visibility="collapsed")

    # Verifica se um novo arquivo foi carregado
    if uploaded_file is not None:
        # Se o nome do arquivo for diferente do último processado, limpa o estado antigo
        if st.session_state.get('last_uploaded_file_name') != uploaded_file.name:
            print("[DEBUG] Novo arquivo detectado. Resetando estado.")
            st.session_state['failed_report_data'] = []
            st.session_state['processing_done'] = False
            st.session_state['last_uploaded_file_name'] = uploaded_file.name

            try:
                match = re.match(r"^(\d{2}\.\d{2}\.\d{4})", uploaded_file.name)
                if match:
                    pay_data = match.group(1)
                    pay_data = re.sub(r"\.", "-", pay_data)
                    pay_data = datetime.strptime(pay_data, '%d-%m-%Y').strftime('%Y-%m-%d')
                    st.session_state["pay_data"] = pay_data

                base_bruta = pd.read_excel(uploaded_file, header=3)
                df = pd.DataFrame()
                df["CPF"] = base_bruta["CPF/CNPJ"].astype(str).str.rjust(11, '0')
                df["Nome"] = base_bruta["NOME COMPLETO"].astype(str)
                df["Valor"] = base_bruta["VALOR"].astype(int)
                st.session_state["df"] = df
            except Exception as e:
                st.error(f"Erro ao ler o arquivo: {e}")
                st.session_state["df"] = None # Limpa o dataframe em caso de erro

    # Mostra o resumo se o dataframe existir no estado da sessão
    if "df" in st.session_state and st.session_state["df"] is not None:
        st.write("Resumo dos Pagamentos:")
        st.dataframe(st.session_state["df"])

def on_click():
    df = st.session_state.get("df")
    pay_data = st.session_state.get("pay_data")
    user = st.session_state.get("login")
    
    failed_report_data = []

    if df is not None:
        progress_bar = st.progress(0, text="Processando pagamentos...")
        total_rows = len(df)
        
        for i, row in df.iterrows():
            c, n, v = row["CPF"], row["Nome"], row["Valor"]
            failed_docs = fb.read(c, v, n, user, pay_data)
            if failed_docs:
                failed_report_data.extend(failed_docs)
            progress_bar.progress((i + 1) / total_rows)
        
        progress_bar.empty()
        st.success("Processamento concluído!")
        
        # Salva os resultados e marca o processamento como concluído
        st.session_state['failed_report_data'] = failed_report_data
        st.session_state['processing_done'] = True
        
        if not failed_report_data:
            st.balloons()
            st.info("Ótima notícia! Nenhum erro encontrado durante o processamento.")

def indication_confirm():
    # Renderiza o botão
    if "df" in st.session_state and st.session_state["df"] is not None:
        st.button(
            "Confirmar Pagamentos",
            on_click=on_click,
            disabled=st.session_state.get('processing_done', False)
        )

    # Renderiza o relatório de falhas se o processamento estiver concluído
    if st.session_state.get('processing_done', False):
        if st.session_state.get('failed_report_data'):
            st.markdown("---")
            st.error(f"Foram encontrados {len(st.session_state['failed_report_data'])} erros durante o processamento.")
            
            report_df = pd.DataFrame(st.session_state['failed_report_data'])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                report_df.to_excel(writer, index=False, sheet_name='Falhas_Pagamento')
            
            st.download_button(
                label="📥 Baixar Relatório de Falhas",
                data=output.getvalue(),
                file_name="relatorio_falhas_pagamento.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )