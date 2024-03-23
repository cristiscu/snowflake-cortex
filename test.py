# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

from snowflake.ml.modeling.preprocessing import OneHotEncoder
help(OneHotEncoder)