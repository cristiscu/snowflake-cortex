import streamlit as st
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

st.header("English-to-French Translator")

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
if prompt := st.text_area(" ", value="How are you doing?", height=300):
    prompt = prompt.replace("'", "''")
    query = f"SELECT snowflake.cortex.translate('{prompt}', 'en', 'fr')"
    response = session.sql(query).collect()[0][0]
    st.write(response)
