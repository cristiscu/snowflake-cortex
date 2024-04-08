import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
query = """SELECT latitude::float as lat,
   longitude::float as lon,
   round(median_house_value / 10000) as val
FROM TEST.PUBLIC.HOUSING
ORDER BY val DESC"""
df = session.sql(query).collect()
st.map(df, size="val", use_container_width=True)
