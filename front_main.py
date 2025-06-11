import streamlit as st
import pandas as pd
import numpy as np
from functions.objects import *


# with st.sidebar:
#     st.sidebar.title("Menu")
#     st.sidebar.write("Selecione uma opção abaixo:")
#     st.sidebar.button("Início")

upload_file()
indication_confirm()

st.markdown(
    """
    <div style="text-align: center; font-size: 12px; color: gray; margin-top: 50px;">
        Feito por Cris
    </div>
    """,
    unsafe_allow_html=True
)