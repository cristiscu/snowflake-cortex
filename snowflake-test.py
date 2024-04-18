# =======================================================================
# Snowpark version
from snowflake.snowpark.version import VERSION
ver = VERSION
print(f'Snowpark for Python version: {ver[0]}.{ver[1]}.{ver[2]}')

# =======================================================================
# connect to your Snowflake account with Python Connector
import os, configparser

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
section = "connections.test_conn"

import snowflake.connector
conn = snowflake.connector.connect(
    account=parser.get(section, "accountname"),
    user=parser.get(section, "username"),
    password=parser.get(section, "password"))

cur = conn.cursor()
cur.execute("SHOW PARAMETERS in SESSION")
for row in cur:
    print(f'{str(row[0])}={str(row[1])}')

# =======================================================================
# connect to your Snowflake account with Snowpark

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
pars = {
    "account": parser.get(section, "accountname"),
    "user": parser.get(section, "username"),
    "password": parser.get(section, "password")}

from snowflake.snowpark import Session
session = Session.builder.configs(pars).create()

res = session.sql('SELECT current_user(), current_version()').collect()
print(f'User: {res[0][0]}')

# =======================================================================
# connect to your Snowflake account with Snowpark ML
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
pars = SnowflakeLoginOptions("test_conn")
session = Session.builder.configs(pars).create()

session.sql_simplifier_enabled = True
print(f'Role: {session.get_current_role()}')
print(f'Database: {session.get_current_database()}')
print(f'Schema: {session.get_current_schema()}')
print(f'Warehouse: {session.get_current_warehouse()}')

res = session.sql('SELECT current_user(), current_version()').collect()
print(f'User: {res[0][0]}')
print(f'Snowflake version: {res[0][1]}')

from snowflake.ml.modeling.preprocessing import OneHotEncoder
help(OneHotEncoder)