from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.registry import Registry

def get_registry():
    pars = SnowflakeLoginOptions("test_conn")
    session = Session.builder.configs(pars).create()
    return Registry(session=session)

def get_model(name):
    return get_registry().get_model(name)
