import streamlit as st
import polars as pl
import json
from SalesforceHelper import SalesforceHelper


# Connexion à sf depuis le cache streamlit
sf = SalesforceHelper.sf_connect_cached()

st.title("🔀 Mise à jour d'objets Salesforce")

# 1. Choix de l'objet Salesforce
object_choice = st.selectbox("🗂️ Choisir l'objet salesforce", ["Product2", "PricebookEntry"])

# 2. Upload du fichier CSV
uploaded_file = st.file_uploader("📄 Charger un fichier CSV avec les IDs + des colonnes à actualiser", type="csv")

if uploaded_file:
    df = pl.read_csv(uploaded_file)
    st.write("✅ Fichier chargé :", df.shape)

    st.dataframe(df)

    if st.button("🔀 Actualiser"):
        with st.spinner(f"⏳ Actualisation de {df.height} objets {object_choice}"):
            df_results = sf.update_object_entries_bulk(df, object_choice)

        df_results = df_results.with_columns(pl.col("errors").map_elements(lambda elm: json.dumps(elm.to_list()) if not elm.is_empty() else None, return_dtype=pl.String).alias("errors"))

        st.dataframe(df_results)
        st.success("✅ Actualisation terminée")
