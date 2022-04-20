from sqlite3 import Cursor
import json

def exists_table(cur: Cursor, name: str) -> bool:
    """Method checks if table exists
        Args:
            cur : Cursor Object
            name: string
        Returns: Boolean
        """
    query1 = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
    return cur.execute(query1, (name,)).fetchone() is not None

def append_row(cur: Cursor, row: dict) -> None :
    """Method appends values to table
        Args:
            cur: Cursor Object
            row: dictionary
        Returns: None
        """
    if not exists_table(cur, row["table"]):
        create_table(cur, row["table"], row["columnnames"], row["columntypes"])
    plchold = ', '.join('?' * len(row['columnvalues']))
    insrt_query = f"INSERT INTO `{row['table']}` VALUES({plchold})"
    cur.execute(insrt_query, row['columnvalues'])

def create_table(cur: Cursor, table: str, fields: list, types:list) -> None:
    """Method creates table
        Args:
            cur   : Cursor Object
            table : table name
            fields: list of fields' names
            types : list of fields' types
        Returns: None
        """
    schema = ', '.join([str(a) +' '+ str(b) for a,b in zip(fields,types)])
    query = f"CREATE TABLE IF NOT EXISTS `{table}` ({schema})"
    cur.execute(query)

def get_query_from_dict(dic: dict, cols: list) -> str:
    """Method returns query(string) from dict
        and list
        Args:
            dic   : dictionary
            cols  : list of fields' names
        Returns:
            values: string
            """
    tmp_ls = []
    for key, value in dic.items():
        if key in cols:
            tmp_ls.append(f"{key} =  '{value}'")
    values = ', '.join(tmp_ls)
    return values

def explode_fields(cur: Cursor, table: str, cols: list) -> None:
    """Method explodes nested dictionary into
        columns
        Args:
            cur   : Cursor Object
            table : table name
            cols  : list of fields' names
        Returns: None
            """
    query = f"SELECT * from {table}"
    cur.execute(query)
    rows = cur.fetchall()
    if table == 'event_v2_data':
        loct = 13
    elif table == 'transaction_request':
        loct = 18
    elif table == 'payment_instrument_token_data':
        loct = 6
    for row in rows:
        itm = row[loct]
        try:
            dic = json.loads(itm)
            values = get_query_from_dict(dic, cols)
            update = f"UPDATE {table} SET {values}"
        except:
            values = " = 'null', ".join(cols)
            update = f"UPDATE {table} SET {values} = 'null'"
        cur.execute(update)


def gets_lists() -> list:
    """Method returs two lists
        Args: None
        Returns:
            fields_ls : list
            types_ls  : list
        """
    fields_ls = ["event_id", "flow_id", "created_at",
            "transaction_lifecycle_event",
            "transaction_id", "transaction_type",
            "amount", "currency_code", "processor_merchant_account_id",
            "three_d_secure_authentication", "payment_instrument_type"]
    types_ls = ["uuid", "uuid", "timestamp without time zone",
            "transaction_lifecycle_event_type",
            "uuid", "transaction_type", "integer",
            "character(3)", "uuid", "jsonb",
            "character varying(255)"]
    return fields_ls, types_ls

def get_dict(keys: list, values: list) -> dict:
    """Method returs dictionary out of
         two lists
        Args:
            keys   : list
            values : list
        Returns:
            dictionary : dictionary
            """
    return dict(zip(keys, values))

def add_columns(cur: Cursor, table: str, columns: list, types: list) -> None:
    """Method add field(s) to datalake
        Args:
            cur    : Cursor
            table  : string
            columns: list of columns
            types  : list of types
        Returns: None
            """
    dictionary = get_dict(columns, types)
    for fld in columns:
        query = f"ALTER TABLE {table} ADD COLUMN {fld} '{dictionary[fld]}'"
        cur.execute(query)

def manip_list(op: str, ls: list, itms: list) -> list:
    """Method adds/removes items to/from a list
        Args:
            op  : add or remove
            ls  : list to be manipulated
            itms: items that will be added/removed
        Returns:
            ls  : updated list
        """
    if op == "add":
        ls.extend(itms)
    elif op == "remove":
        for itm in itms:
            ls.remove(itm)
    return ls
