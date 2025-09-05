import yaml
import polars as pl
from typing import List, Dict, Optional, Any
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError
from simple_salesforce.exceptions import SalesforceResourceNotFound
from pathlib import Path
import time
import sys
import pendulum
import streamlit as st

# Ajoute le dossier parent du dossier courant au sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent  # -> sf_techeasy -> py_script_store
sys.path.append(str(ROOT_DIR))
# sys.path.insert(0, "..")
from timer import Timer


class SalesforceHelper:
    def __init__(
        self,
        username: str,
        password: str,
        security_token: str,
        domain: str,
        env: str = "preprod",
    ) -> None:
        self.sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)

        self.api_version = "v52.0"
        self.bulk_url = f"https://{self.sf.sf_instance}/services/data/{self.api_version}/jobs/ingest"
        self.env = env
        self.sf_instance = self.sf.sf_instance

        self.now_str = pendulum.now().format("YYYYMMDD_HHmm")
        self.df_tables: pl.DataFrame = pl.DataFrame()
        self.df_colonnes: pl.DataFrame = pl.DataFrame()

        print(f"Connected to {self.sf.sf_instance}")

    @classmethod
    def from_yml(
        cls,
        yml_path: str = "config.yml",
        yml_key: str = "sf_techeasy",
    ) -> "SalesforceHelper":
        """Crée une instance de SalesforceHelper à partir d'un fichier de configuration YAML."""
        try:
            with open(yml_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)

            env = config[yml_key]["env"]
            db_conf = config[yml_key][env]

            return cls(
                username=db_conf["user"],
                password=db_conf["mdp"],
                security_token=db_conf["token"],
                domain="test" if env == "preprod" else "login",
                env=env,
            )
        except FileNotFoundError:
            print(f"Erreur: Fichier de configuration '{yml_path}' non trouvé.")
            raise
        except yaml.YAMLError as e:
            print(f"Erreur lors du parsing du fichier YAML '{yml_path}': {e}")
            raise
        except Exception as e:  # Pour les erreurs de configuration personnalisées
            print(f"Erreur de clés yaml ou autre : {e}")
            raise

    def get_sf_tables(self) -> pl.DataFrame:
        """Récupération de tous les objets disponibles"""
        if not self.df_tables.is_empty():
            return self.df_tables

        self.df_tables = pl.DataFrame(self.sf.describe()["sobjects"])
        print(f"This sf {self.sf.sf_instance} has {self.df_tables.height} objects")

        return self.df_tables

    def get_sf_columns(self, table_list: List[str] = []) -> pl.DataFrame:
        if len(table_list) == 0:
            if not self.df_colonnes.is_empty():
                return self.df_colonnes
            if not self.df_tables.is_empty():
                table_list = self.get_sf_tables()["name"].to_list()

        print(f"  Fetch columns from {len(table_list)} sf tables")
        cols = []
        start_time = time.time()
        for table_name in table_list:
            obj_description = getattr(self.sf, table_name).describe()["fields"]
            for d in obj_description:
                d["table_name"] = table_name

            cols.extend(obj_description)
            # print(f"    {table_name}: {len(obj_description)} colonnes")

        self.df_colonnes = pl.DataFrame(cols, infer_schema_length=10000)
        print(f"  {self.df_colonnes.height} columns in  ({Timer.get_time_diff(start_time)})")
        return self.df_colonnes

    def get_object_field_names(self, object_name: str):
        """Récupère la liste des noms de champs (colonnes) pour un objet Salesforce spécifié."""
        fields = getattr(self.sf, object_name).describe()["fields"]

        # Champs composés principaux à exclure
        compound_fields = [field["compoundFieldName"] for field in fields if field.get("compoundFieldName")]

        # Champs simples ou sous-champs de composés
        field_names = [field["name"] for field in fields if field["name"] not in compound_fields]
        return field_names, compound_fields

    def query_all(self, object_name: str, field_names: Optional[List[str]] = [], limit: int = 100) -> pl.DataFrame:
        start_time = time.time()
        print(f"Let's fetch {object_name}")
        if len(field_names) == 0:
            field_names, compound_fields = self.get_object_field_names(object_name)

        query = f"SELECT {', '.join(field_names + compound_fields)} FROM {object_name} limit {limit}"
        records = self.sf.query_all(query)["records"]

        df = pl.DataFrame(records, infer_schema_length=10000)
        print("  ", df.height, f" rows fetched in {object_name} ({Timer.get_time_diff(start_time)})")
        return df

    def fetch_bulk_v2(self, object_name: str):
        start_time = time.time()

        sf_obj = getattr(self.sf.bulk2, object_name)
        field_names, compound_fields = self.get_object_field_names(object_name)

        print(f"  Let's fetch {object_name}\t except {compound_fields=}")

        data_path = f"offload/{self.env}_{self.now_str}/{object_name}/"
        Path(data_path).mkdir(parents=True, exist_ok=True)

        query = f"SELECT {', '.join(field_names)} FROM {object_name}"
        sf_obj.download(query, path=data_path, max_records=50000)

        print("    ", object_name, f" downloaded ({Timer.get_time_diff(start_time)})")

    def update_object_entries_bulk(self, df: pl.DataFrame, object_name: str) -> pl.DataFrame:
        """update Salesforce Object entries 1 by 1 - slow vs bulk"""
        start_time = time.time()

        assert "Id" in df.columns, "La colonne 'Id' est absente du DataFrame"
        sf_obj = getattr(self.sf.bulk, object_name)
        sf_obj_cols, _ = self.get_object_field_names(object_name)

        # Colonnes non autorisées
        cols_not_in_sf_obj = [col for col in df.columns if col not in sf_obj_cols]
        assert len(cols_not_in_sf_obj) == 0, f"Colonnes non autorisées détectées : {cols_not_in_sf_obj}"

        print(f"🔀 Update {object_name} entries bulk : {df.height} lignes")
        res = sf_obj.update(df.to_dicts(), batch_size=10000, use_serial=True)
        res = pl.DataFrame(res)

        print("  ", res.filter(pl.col("success")).height, f"lignes mises à jour ({Timer.get_time_diff(start_time)})")

        return res

    @st.cache_resource
    def sf_connect_cached() -> "SalesforceHelper":
        """Connexion à Salesforce"""
        return SalesforceHelper.from_yml()
