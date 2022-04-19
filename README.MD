# Data Engineer Challenge

This repo contains the solution for Primer's Data Challenge

### How To Run

Python 3 is required for running this application.
Clone the repo and then run `python3 main.py`


## Solution

In order to create the metric, the program reads and iterates through the `wal.json` so it 
can be flatened and create tables. The program uses a custom library of `sql_tools` where all the sql/json related operations take place (steps):
- Iterate through the json and ensure that only data from `insert` actions are ingested
- In each row iteration there is a check whether the respective table (can be found in the row) already exists. If it does not exist, the table gets created automatically
- After ensuring the existence of the table the program inserts the respective values
- According to the assignment, nested dictionaries have to be exploded the required fields can be generated. For this operation the program uses the custom functions `add_columns` and `explode_fields`. These two functions run for all the three tables
- Next step is creating the table that will host the outcome. Fields and respective types are retrieved from a function and are passed as arguments in the function for creating the table
- Following the previous approach (adding new columns), new columns are added to the new table so it matches the desired output
- Using a custom function to add/remove elements in a list, we generate a list of the fields matching the outcome's schema
- Last step is calling a function that performs the required join and write that table into table `outcome` in the `metrics.db`

## Assumptions

Since no information was given around the system where this code is running, 
certain assumptions were made:
- There is no sql-injection threat: For generating queries, `f-string` was used (for automation purposes)
- Assumed that the program is not getting tables' information from `sys.args` or somewhere else and therefore fields/types are hardcoded in many cases
- Only this join takes place and therefore big part of it is hardcoded







