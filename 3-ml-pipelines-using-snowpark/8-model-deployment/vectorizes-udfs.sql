use schema test.public;

create or replace function add_2(x float, y float)
    returns float
    language python
    runtime_version = 3.9
    handler = 'add_2'
as $$
def add_2(x, y):
  return x + y
$$;

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

create or replace table xy(x float, y float);
insert into xy values (1.0, 3.14), (2.2, 1.59), (3.0, -0.5);

select x, y, add_2(x, y), add_v(x, y)
from xy;