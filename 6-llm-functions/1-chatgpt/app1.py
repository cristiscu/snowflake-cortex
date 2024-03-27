# test with "generate SQL query to return all schema names"

import os
import streamlit as st
from openai import OpenAI

st.header("Snowflake Q&A Metadata Inspector")

label = "Ask some question about the SNOWFLAKE_SAMPLE_DATA database and press Enter:"
if prompt := st.text_input(label):
    prompt = f"""
Respond with one single Snowflake query that returns"
metadata from the SNOWFLAKE_SAMPLE_DATA database"
using the INFORMATION_SCHEMA: {prompt}
"""
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    r = client.chat.completions.create(
        model="gpt-4-1106-preview",
        messages=[{"role": "user", "content": prompt}])
    response = r.choices[0].message.content
    st.write(response)
