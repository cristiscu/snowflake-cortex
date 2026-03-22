import pandas as pd
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

emps = session.table("EMP").to_pandas()[["DEPTNO", "SAL"]]
depts = session.table("DEPT").to_pandas()[["DEPTNO", "DNAME"]]
df = pd.merge(emps, depts, on="DEPTNO", how="inner")[["DNAME", "SAL"]]

df = df[~(df["DNAME"] == 'RESEARCH')]
df = df.groupby(["DNAME"]).sum()
df = df.sort_values("DNAME")
print(df)
