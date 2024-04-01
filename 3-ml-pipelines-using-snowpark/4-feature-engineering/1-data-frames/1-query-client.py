import os, configparser
import pandas as pd
import snowflake.connector
import queries

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
section = "connections.test_conn"
conn = snowflake.connector.connect(
    account=parser.get(section, "accountname"),
    user=parser.get(section, "username"),
    password=parser.get(section, "password"))

df = pd.read_sql(queries.target_query, conn)
print(df)