from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
session.sql_simplifier_enabled = True

emps = session.table("EMP").select("DEPTNO", "SAL")
depts = session.table("DEPT").select("DEPTNO", "DNAME")
q = emps.join(depts, emps.deptno == depts.deptno)

q = q.filter(q.dname != 'RESEARCH')
q = q.select("DNAME", "SAL").group_by("DNAME").agg({"SAL": "sum"}).sort("DNAME")
q.show()

print(q.queries)
"""
-- generated query:

SELECT * FROM
  (SELECT "DNAME", sum("SAL") AS "SUM(SAL)"
  FROM (SELECT "DNAME", "SAL" FROM (
    SELECT * FROM ((SELECT "DEPTNO" AS "l_vj35_DEPTNO", "SAL" AS "SAL" FROM EMP) AS SNOWPARK_LEFT
      INNER JOIN (SELECT "DEPTNO" AS "r_n6m2_DEPTNO", "DNAME" AS "DNAME" FROM DEPT) AS SNOWPARK_RIGHT
      ON ("l_vj35_DEPTNO" = "r_n6m2_DEPTNO")))
  WHERE ("DNAME" != \'RESEARCH\'))
  GROUP BY "DNAME")
  ORDER BY "DNAME" ASC NULLS FIRST
"""
