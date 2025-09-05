from SalesforceHelper import SalesforceHelper
import polars as pl
from pathlib import Path
import time
import sys
# import duckdb

# Ajouter le dossier parent au sys.path
sys.path.insert(0, "..")
from timer import Timer

# Connexion à Salesforce
sf = SalesforceHelper.from_yml()

# list all tables
df_sf_tables = sf.get_sf_tables()
df_sf_tables.write_parquet(f"offload/df_sf_tables_{sf.env}.parquet")


# -----------------------------------------------------------------------------
# bulk_2 exports -< outputs .csv chuncks of sf table
# -----------------------------------------------------------------------------

# sf.fetch_bulk_v2("Opportunity2__c")
# sf.fetch_bulk_v2("Devis_type__c")
# sf.fetch_bulk_v2("Lignes_de_devis_type__c")
# sf.fetch_bulk_v2("Product2")
sf.fetch_bulk_v2("PricebookEntry")

# -----------------------------------------------------------------------------
# query_all exports -> slower
# -----------------------------------------------------------------------------

# def query_and_offload(table: str) -> pl.DataFrame:
#     df = sf.query_all(table, limit=100000).select(pl.exclude("attributes")).cast(pl.String)
#     print(f"{df=}")
#     df.write_csv("offload/df_{table}_{sf.env}.csv")
#     df.write_parquet(f"offload/df_{table}_{sf.env}.parquet")
#     # duckdb.sql("copy (from df) to 'offload/df_Product2.csv'")


# query_and_offload('Product2')
# query_and_offload('Devis_type__c')
# query_and_offload('Lignes_de_devis_type__c')
# query_and_offload("Opportunity2__c")
