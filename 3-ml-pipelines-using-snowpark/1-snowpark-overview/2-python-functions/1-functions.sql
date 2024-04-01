use schema test.public;

-- Python stored procedure
-- https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-python
create or replace procedure procPythonS(num float)
  returns string
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'proc1'
as $$
import snowflake.snowpark as snowpark
def proc1(session: snowpark.Session, num: float):
  query = f"select '+' || to_char({num})"
  return session.sql(query).collect()[0][0]
$$;

call procPythonS(22.5);

-- Python UDF
-- https://cristian-70480.medium.com/how-to-generate-snowflake-stored-procs-via-python-worksheets-01d49b5b3cb2
create or replace function fctPythonS(num float)
  returns string
  language python
  runtime_version = '3.8'
  -- packages = ('snowflake-snowpark-python')
  handler = 'proc1'
as $$
# import snowflake.snowpark as snowpark
def proc1(num: float):
  return '+' + str(num)
$$;

select fctPythonS(22.5);

-- Python UDTF
-- https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions
create or replace function fcttPythonS(s string)
  returns table(out varchar)
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'MyClassS'
as $$
import snowflake.snowpark as snowpark
class MyClassS:
  def process(self, s: str):
    yield (s,)
    yield (s,)
$$;

select * from table(fcttPythonS('abc'));


-- HANDLER required everywhere (w/ fct name / MyClass w/o process for UDTFs)
-- stored procs always w/ Snowpark
-- packages and import will add Snowpark
-- no session param on UDF/UDTF with Snowpark

-- =================================================================
-- Vectorized Python UDF
create or replace function add_v(x float, y float)
    returns float
    language python
    runtime_version = 3.9
    packages = ('pandas')
    handler = 'add_v'
as $$
import pandas
from _snowflake import vectorized

@vectorized(input=pandas.DataFrame)
def add_v(df):
  return df[0] + df[1]
$$;

create or replace function add_2(x float, y float)
    returns float
    language python
    runtime_version = 3.9
    handler = 'add_2'
as $$
def add_2(x, y):
  return x + y
$$;

create or replace table xy(x float, y float);
insert into xy values (1.0, 3.14), (2.2, 1.59), (3.0, -0.5);

select x, y, add_2(x, y), add_v(x, y)
from xy;