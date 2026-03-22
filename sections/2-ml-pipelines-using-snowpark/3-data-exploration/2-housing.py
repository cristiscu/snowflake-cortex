import streamlit as st
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

query = """
SELECT latitude::float as lat,
   longitude::float as lon,
   round(median_house_value / 10000) as val
FROM TEST.PUBLIC.HOUSING
ORDER BY val DESC
"""

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
df = session.sql(query).collect()
st.map(df, size="val", use_container_width=True)
