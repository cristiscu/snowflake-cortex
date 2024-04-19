use schema test.public;

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

@vectorized(input=pandas.DataFrame, max_batch_size=100)
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