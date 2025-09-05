
-- list of raw.sf_% tables in BigQuery my_gcp_project
with description as (
  select
    table_catalog, table_schema, table_name,
    trim(option_value, '"') as description
  from `my_gcp_project.region-EU.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE table_schema = 'raw' and option_name='description'
), labels as (
  select table_catalog, table_schema, table_name,
    array(
      select as struct arr[offset(0)] key, arr[offset(1)] value
      from unnest(regexp_extract_all(option_value, r'STRUCT\(("[^"]+", "[^"]+")\)')) kv,
      unnest([struct(split(replace(kv, '"', ''), ', ') as arr)])
    ) labels
  from `my_gcp_project.region-eu.INFORMATION_SCHEMA.TABLE_OPTIONS`
  where table_schema = 'raw' and option_name='labels'
)
select
  t.table_catalog, t.table_schema, t.table_name,
  t.table_type,
  datetime_trunc(t.creation_time, second) dth_creation,
  datetime_trunc(s.storage_last_modified_time, second) dth_update,
  s.total_rows as row_count,
  s.total_logical_bytes ,
  round(s.total_logical_bytes/pow(10,9), 2) size_gb,
  d.description table_description,
  TO_JSON(labels.labels) table_labels,
  s.total_partitions,
from `my_gcp_project.region-EU.INFORMATION_SCHEMA.TABLES` t
  left join description d using(table_catalog, table_schema, table_name)
  left join labels using(table_catalog, table_schema, table_name)
  left join `my_gcp_project.region-EU.INFORMATION_SCHEMA.TABLE_STORAGE` s using(table_catalog, table_schema, table_name)
where table_schema = 'raw'
  and table_name like 'sf_%'
