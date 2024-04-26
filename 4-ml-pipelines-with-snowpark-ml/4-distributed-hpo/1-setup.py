# make 20K samples for regression
# see https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset

from sklearn import datasets
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

df = datasets.fetch_california_housing(as_frame=True).frame
df.columns = [c.upper() for c in df.columns]

# pandas Dataframe --> Snowpark DataFrame (in temp table) --> write_pandas() to persist!
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.create_dataframe(df)
df.write.mode("overwrite").save_as_table("CALIFORNIA_HOUSING")
df.show()
