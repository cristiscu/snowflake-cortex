# to deploy as a Streamlit in Snowflake App,
# after creating the openai function w/ setup.sql!

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.header("Basic ChatGPT Q&A Interface")

if prompt := st.text_input(
    label="Ask a question and click Enter:",
    # what is snowflake cortex
    # what are the llm functions in snowflake cortex
    # what are the ml-based functions in snowflake
    # what is snowpark
    value="What is Snowflake Data Cloud"):
    prompt = prompt.replace("'", "''")
    query = f"select openai_db.public.openai('{prompt}')"
    response = get_active_session().sql(query).collect()[0][0]
    st.write(response)
