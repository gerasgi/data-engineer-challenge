import sqlite3
import json

from tools.etl_tools import join_tbls
from tools.sql_tools import *

con = sqlite3.connect("metrics.db")
cur = con.cursor()

# Read the WAL records
with open("./wal.json", "r") as f:
    records = json.loads(f.read())

for item in records:
    for data_item in item["change"]:
        if data_item["kind"] == "insert":
            append_row(cur, data_item)

# Explode nested dic and extend table's schema
add_columns(cur, "event_v2_data",
            ["decline_reason", "decline_type"],
            ["character varying(255)", "character varying(255)",])
explode_fields(cur, "event_v2_data", ["decline_reason", "decline_type"])

add_columns(cur, "transaction_request", ["payment_method"],
            ["character varying(255)"])
explode_fields(cur, "transaction_request", ["payment_method"])

add_columns(cur, "payment_instrument_token_data", ["customer_id"],
            ["character varying(255)"])
explode_fields(cur, "payment_instrument_token_data", ["customer_id"])

# Fields and types of new table
fields, types_ls = gets_lists()
create_table(cur, table="outcome", fields = fields, types=types_ls)

add_columns(cur, "outcome",
            ["decline_reason", "decline_type", "payment_method", "customer_id"],
            ["character varying(255)", "character varying(255)",
            "character varying(255)", "character varying(255)"])

fields_upd = manip_list("add", fields,
            ["customer_id", "decline_reason", "decline_type", "payment_method"])

# ETL
join_tbls(cur, "outcome", fields_upd)
con.commit()
