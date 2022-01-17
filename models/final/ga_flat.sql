{{ config(
    materialized='incremental',
    partition_by={
      "field": "table_date",
      "data_type": "date"},
    cluster_by= ["table_date","user_pseudo_id","event_name"],
    incremental_strategy = 'insert_overwrite'
)}}

