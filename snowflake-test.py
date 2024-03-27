# =======================================================================
# connect to your Snowflake account with Python Connector
import os, configparser
import snowflake.connector

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
section = "connections.test_conn"
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
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

from snowflake.ml.modeling.preprocessing import OneHotEncoder
help(OneHotEncoder)