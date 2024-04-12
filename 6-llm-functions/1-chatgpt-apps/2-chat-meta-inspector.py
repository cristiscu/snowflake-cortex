# test with "generate SQL query to return all schema names"

import re, os
import streamlit as st
from openai import OpenAI

st.title("Snowflake Chat Metadata Inspector")

# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

# connect to your ChatGPT account
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

first = "messages" not in st.session_state
if first:
    st.session_state.messages = [{"role": "system", "content": 
        ("Respond with one single Snowflake query that returns"
        + " metadata from the SNOWFLAKE_SAMPLE_DATA database"
        + f" using the INFORMATION_SCHEMA.")}]

if prompt := st.chat_input(placeholder="Ask a question about Snowflake metadata"):
    st.session_state.messages.append({"role": "user", "content": prompt})

if not first and st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        r = client.chat.completions.create(
        model="gpt-4-turbo-2024-04-09",
        messages=([{"role": m["role"], "content": m["content"]}
            for m in st.session_state.messages]))
        response = r.choices[0].message.content
        st.write(response)

        message = {"role": "assistant", "content": response}
        if sql_match := re.search(r"```sql\n(.*)\n```", response, re.DOTALL):
            query = sql_match.group(1)
            st.divider()
            st.write("Generated query:")
            st.code(query)
            message["results"] = st.dataframe(
                session.sql(query).to_pandas(), use_container_width=True)
        else:
            st.error("No query!")
        st.session_state.messages.append(message)
