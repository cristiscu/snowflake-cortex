import os, configparser
parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
section = "connections.test_conn"

import snowflake.connector
conn = snowflake.connector.connect(
    account=parser.get(section, "accountname"),
    user=parser.get(section, "username"),
    password=parser.get(section, "password"),
    database=parser.get(section, "database"),
    schema=parser.get(section, "schema"))

query = """
select dname, sum(sal)
  from emp join dept on emp.deptno = dept.deptno
  where dname <> 'RESEARCH'
  group by dname
  order by dname;
"""
import pandas as pd
df = pd.read_sql(query, conn)
print(df)