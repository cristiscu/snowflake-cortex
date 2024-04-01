# see https://medium.com/snowflake/how-to-create-a-complex-query-with-snowpark-dataframe-in-python-2d31b9e0961b

target_query = """
select dname, sum(sal)
  from emp join dept on emp.deptno = dept.deptno
  where dname <> 'RESEARCH'
  group by dname
  order by dname;
"""

generated_query = """
SELECT "EMPNO", "DNAME"
FROM (
  SELECT *
  FROM (
    (SELECT "EMPNO" AS "EMPNO", "DEPTNO_E" AS "DEPTNO_E"
    FROM (SELECT "EMPNO", "DEPTNO" AS "DEPTNO_E" FROM EMP)) AS SNOWPARK_LEFT
    INNER JOIN 
      (SELECT "DEPTNO_D" AS "DEPTNO_D", "DNAME" AS "DNAME"
      FROM (SELECT "DEPTNO" AS "DEPTNO_D", "DNAME" FROM DEPT)) AS SNOWPARK_RIGHT
    ON ("DEPTNO_E" = "DEPTNO_D")))
    WHERE ("DNAME" != 'RESEARCH')
    LIMIT 10
"""