# Data Engineer Challenge

This repo contains the solution for Primer's Data Challenge

### How To Run

Python 3 is required for running this application.
Clone the repo and then run `python3 main.py`

## Assumptions

Since no information was given around the system where this code is running, 
certain assumptions were made:
- There is no sql-injection threat: For generating queries, `f-string` was used (for automation purposes)
- The programme is not getting tables' information from `sys.args` or somewhere else and therefore fields/types are hardcoded in many cases
- Only this join takes place and therefore a big part of it is hardcoded

## Solution

In order to create the metric, the programme reads and iterates through the `wal.json`, so it 
can be flattened and create tables. The programme uses the custom library `sql_tools` where all the sql/json related operations take place (steps).
Below are the steps:
- Iterate through the json and ensure that only data from `insert` actions are ingested
- With each iteration of each row, there is a check whether the respective table (which can be found in the row) already exists. If it does not exist, the table is created automatically
- After ensuring the existence of the table, the programme inserts the respective values
- According to the assignment, nested dictionaries have to be exploded so that the required fields can be generated. For this operation, the programme uses the custom functions `add_columns` and `explode_fields`. These two functions run for the three tables
- The next step is creating the table that will host the result. Fields and respective types are retrieved from a function and are passed as arguments in the function to create the table
- Following the previous approach (adding new columns), new columns are added to the new table so it matches the desired output
- Using a custom function to add/remove elements in a list, the programme generates a list of the fields matching the outcome's schema
- The last step is calling a function that performs the required join and writes the result into table `outcome`, in the `metrics.db`

In order to get the data in proper format(flattened i.e. list) the programme iterates through the `wal.json`. During the iteration, a check is performed on whether or not the table is an `insert` table. 
In each iteration, function `append_row` is called. This function inserts the data to the respective table. For that to be done, a check, whether or not this table exists, takes place. If it does not exist, the same function takes on to create the necessary table.
After having the tables in place, the programme alters the schema by adding fields. This is necessary as some of the fields consist of nested objects which need to be exploded and populated accordingly. This is done by calling the `add_columns()` function. The next step is to "explode" the necessary fields using the function(`explode_fields()`). The exact location of the objects in the list is hardcoded.
All tables have all the necessary fields. The next step is the target table `outcome`(where the result will be stored). The programme uses the `create_table()` function to create the table (this is done by getting two hardcoded lists through a function). Then, the programme alters the schema by using again the `add_rows()`.
The last part of the programme is the join between the tables. This is carried out by a function that executes the required join.
Note:
The programme uses `f-Strings` for automation purposes (placeholders in `sqlite3` have a limitation when it comes to tables/columns names - it is always strongly recommended to use tuples as to avoid sql-injections)

## Outcome
The outcome is table `outcome` in `metrics` database. It consists of 529 records. Its schema is the following:
```
(0, 'event_id', 'uuid', 0, None, 0), 
(1, 'flow_id', 'uuid', 0, None, 0),
(2, 'created_at', 'timestamp without time zone', 0, None, 0), 
(3, 'transaction_lifecycle_event', 'transaction_lifecycle_event_type', 0, None, 0), 
(4, 'transaction_id', 'uuid', 0, None, 0), 
(5, 'transaction_type', 'transaction_type', 0, None, 0), 
(6, 'amount', 'INTEGER', 0, None, 0), 
(7, 'currency_code', 'character(3)', 0, None, 0), 
(8, 'processor_merchant_account_id', 'uuid', 0, None, 0), 
(9, 'three_d_secure_authentication', 'jsonb', 0, None, 0), 
(10, 'payment_instrument_type', 'character varying(255)', 0, None, 0), 
(11, 'decline_reason', 'character varying(255)', 0, None, 0), 
(12, 'decline_type', 'character varying(255)', 0, None, 0), 
(13, 'payment_method', 'character varying(255)', 0, None, 0), 
(14, 'customer_id', 'character varying(255)', 0, None, 0)```
