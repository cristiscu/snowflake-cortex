from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

emps = (session.table("EMP").select("DEPTNO", "SAL"))
depts = (session.table("DEPT").select("DEPTNO", "DNAME"))
q = emps.join(depts, emps.deptno == depts.deptno)

q = q.filter(q.dname != 'RESEARCH')
(q.select("DNAME", "SAL")
  .group_by("DNAME")
  .agg({"SAL": "sum"})
  .sort("DNAME")
  .show())
