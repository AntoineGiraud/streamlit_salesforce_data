from SalesforceHelper import SalesforceHelper
import polars as pl
import duckdb
from pathlib import Path
import time
import sys
from sf_techeasy.offload.sf_tables_to_offload import sf_tables_to_offload

# Ajouter le dossier parent au sys.path
sys.path.insert(0, "..")
from timer import Timer

# Connexion à Salesforce
sf = SalesforceHelper.from_yml()

df_sf_tables = sf.get_sf_tables()
df_sf_tables.write_parquet(f"offload/df_sf_tables_{sf.env}.parquet")

df_sf_columns = sf.get_sf_columns(table_list=[table["name"] for table in sf_tables_to_offload])
df_sf_columns.write_parquet(f"offload/df_sf_table_columns_{sf.env}.parquet")

sf_tables = df_sf_tables["name"].to_list()

# Tables à exporter inconnue dans salesforce
unknown_sf_tables_inSelection = [table["name"] for table in sf_tables_to_offload if table["name"] not in sf_tables]
assert len(unknown_sf_tables_inSelection) == 0, f"Tables à exporter inconnues de votre Salesforce {unknown_sf_tables_inSelection}"

# -------------------------------------------------------------
# Export de la sélection de tables salesforce
# -------------------------------------------------------------
print(f"Let's offload {len(sf_tables_to_offload)} tables from {sf.sf.sf_instance}")

for table in sf_tables_to_offload:
    table_name = table["name"]
    print(f"  ⤵️  Download {table_name} (~{table['rows']})")

    # if table["rows"] > 50_000:
    #     print("      an other day with better wifi")
    #     continue

    sf.fetch_bulk_v2(table["name"])

    # convert to .parquet
    table_path = f"./offload/{sf.env}_{sf.now_str}"
    duckdb.sql(f"copy(from '{table_path}/{table_name}/*.csv') to '{table_path}/{table_name}.parquet'")
