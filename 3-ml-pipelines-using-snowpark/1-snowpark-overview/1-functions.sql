use schema test.public;

-- Python stored procedure
-- https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-python
create or replace procedure proc1(num float)
  returns string
  language python
  runtime_version = '3.9'
  packages = ('snowflake-snowpark-python==1.13.0')
  handler = 'proc1'
as $$
import snowflake.snowpark as snowpark
def proc1(session: snowpark.Session, num: float):
  query = f"select '+' || to_char({num})"
  return session.sql(query).collect()[0][0]
$$;

call proc1(22.5);

-- Python UDF
-- https://cristian-70480.medium.com/how-to-generate-snowflake-stored-procs-via-python-worksheets-01d49b5b3cb2
create or replace function proc2(num float)
  returns string
  language python
  runtime_version = '3.9'
  -- packages = ('snowflake-snowpark-python')
  handler = 'proc2'
as $$
# import snowflake.snowpark as snowpark
def proc2(num: float):
  return '+' + str(num)
$$;

select proc2(22.5);

-- Python UDTF
-- https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions
create or replace function proc3(s string)
  returns table(out varchar)
  language python
  runtime_version = '3.9'
  -- packages = ('snowflake-snowpark-python')
  handler = 'MyClassS'
as $$
# import snowflake.snowpark as snowpark
class MyClassS:
  def process(self, s: str):
    yield (s,)
    yield (s,)
$$;

select * from table(proc3('abc'));
