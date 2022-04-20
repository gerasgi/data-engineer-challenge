from sqlite3 import Cursor

def join_tbls(cur: Cursor, table:str, fields: list ) -> None:
    """Method performs joins on tables
        Args:
            cur   : Cursor Object
            table : table name
            fields: list of table's fields
        Returns: None """
    query = f"""INSERT INTO {table} ({','.join(fields)})
                select event_v2_data.event_id AS event_id, 
                       event_v2_data.flow_id AS flow_id, 
                       event_v2_data.created_at AS created_at, 
                       event_v2_data.transaction_lifecycle_event AS transaction_lifecycle_event, 
                       `transaction`.transaction_id AS transaction_id, 
                       `transaction`.transaction_type AS transaction_type, 
                       `transaction`.amount AS amount, 
                       `transaction`.currency_code AS currency_code, 
                       `transaction`.processor_merchant_account_id AS processor_merchant_account_id,
                        payment_instrument_token_data.three_d_secure_authentication AS three_d_secure_authentication, 
                        payment_instrument_token_data.payment_instrument_type AS payment_instrument_type, 
                        payment_instrument_token_data.customer_id AS customer_id,
                        event_v2_data.decline_reason AS decline_reason,
                        event_v2_data.decline_type AS decline_reason,
                        transaction_request.payment_method AS payment_method
                FROM event_v2_data
                    JOIN `transaction` ON 
                        event_v2_data.transaction_id = `transaction`.transaction_id
                    JOIN transaction_request ON 
                        event_v2_data.flow_id = `transaction_request`.flow_id
                    JOIN payment_instrument_token_data ON 
                        transaction_request.token_id = payment_instrument_token_data.token_id"""
    cur.execute(query)
