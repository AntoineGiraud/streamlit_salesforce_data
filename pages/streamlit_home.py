import streamlit as st
import polars as pl
from SalesforceHelper import SalesforceHelper


# Connexion à sf depuis le cache streamlit
sf = SalesforceHelper.sf_connect_cached()

# Titre principal
st.title("🧰 Actualisations Salesforce")


st.markdown("""
Cette app streamlit va vous permettre
* d'actualiser une liste d'objets salesforce en passant un .csv avec `Id,Colonne1àActualiser,Colonne2àActualiser`
* de supprimer ou désactiver une liste d'Id salesforce en passant un .csv
""")


# Chargement et mise en cache des données
@st.cache_data
def get_data():
    return sf.get_sf_tables()


df_sf_tables = get_data()

# Aperçu des données
with st.expander("Aperçu des objets salesforce"):
    st.dataframe(df_sf_tables.head(50), use_container_width=True)
