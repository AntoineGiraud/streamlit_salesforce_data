import streamlit as st
import polars as pl
import json
from SalesforceHelper import SalesforceHelper


# Connexion à sf depuis le cache streamlit
sf = SalesforceHelper.sf_connect_cached()

st.title("🧹 Ménage d'objets Salesforce")

# 1. Choix de l'objet Salesforce
object_choice = st.selectbox("🗂️ Choisir l'objet salesforce", ["Product2", "PricebookEntry"])

# 2. Upload du fichier CSV
uploaded_file = st.file_uploader("📄 Charger un fichier CSV avec les IDs", type="csv")

if uploaded_file:
    df_ids = pl.read_csv(uploaded_file)
    st.write("✅ Fichier chargé :", df_ids.shape)

    if st.button("🧊 Désactiver"):
        with st.spinner(f"⏳ Désactivation de {df_ids.height} objets {object_choice}"):
            # Préparation des données
            df_ids = df_ids.rename({df_ids.columns[0]: "Id"}).with_columns(pl.lit(False).alias("IsActive"))
            print(f"{df_ids=}")

            sf_obj_bulk = getattr(sf.sf.bulk, object_choice)
            df_results = pl.DataFrame(sf_obj_bulk.update(df_ids.to_dicts()))
            print(f"{df_results=}")

        df_results = df_results.with_columns(pl.col("errors").map_elements(lambda elm: json.dumps(elm.to_list()) if not elm.is_empty() else None, return_dtype=pl.String).alias("errors"))

        st.dataframe(df_results)
        st.success("✅ Désactivation terminée")

    st.divider()

    if st.button("❌ Supprimer"):
        with st.spinner(f"⏳ Suppression de {df_ids.height} objets {object_choice}"):
            # Préparation des données
            ids_to_delete = df_ids.select(pl.col(df_ids.columns[0])).to_series().to_list()

            sf_obj_bulk = getattr(sf.sf.bulk, object_choice)
            df_results = pl.DataFrame(sf_obj_bulk.delete(ids_to_delete))

        df_results = df_results.with_columns(pl.col("errors").map_elements(lambda elm: json.dumps(elm.to_list()) if not elm.is_empty() else None, return_dtype=pl.String).alias("errors"))

        st.dataframe(df_results)
        st.success("✅ Suppression terminée")
