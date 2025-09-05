import streamlit as st

from SalesforceHelper import SalesforceHelper


# Connexion à sf depuis le cache streamlit
sf = SalesforceHelper.sf_connect_cached()


# Define the pages
main_page = st.Page("pages/streamlit_home.py", title="Accueil", icon="🎈")
page_2 = st.Page("pages/streamlit_sf_dropOrDesactive.py", title="Ménage d'objets Salesforce", icon="🧹")
page_3 = st.Page("pages/streamlit_sf_update.py", title="Mise à jour d'objets Salesforce", icon="🔀")

# Set up navigation
pg = st.navigation([main_page, page_2, page_3])

# Sidebar pour les filtres généraux
st.sidebar.markdown("Connected to")
st.sidebar.markdown(f"`{sf.sf_instance}`")

# Run the selected page
pg.run()
