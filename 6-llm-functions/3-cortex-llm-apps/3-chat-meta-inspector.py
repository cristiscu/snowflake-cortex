import re, json
import streamlit as st
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

st.title("Snowflake Chat Metadata Inspector")

session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
first = "messages" not in st.session_state
if first:
    st.session_state.messages = [{"role": "system", "content": 
        ("Respond with one single Snowflake query that returns"
        + " metadata from the SNOWFLAKE_SAMPLE_DATA database"
        + " using the INFORMATION_SCHEMA.")}]

# generate SQL query to return all schema names
# generate SQL query to return first schema name
if prompt := st.chat_input(placeholder="Ask a question about Snowflake metadata"):
    prompt = prompt.replace("'", "''")
    st.session_state.messages.append({"role": "user", "content": prompt})

if not first and st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        messages=[{'role': m['role'], 'content': m['content']}
            for m in st.session_state.messages]
        arg = str(messages).replace('"', "'")
        query = f"select snowflake.cortex.complete('mistral-large', {arg}, {{}})"
        response = json.loads(session.sql(query).collect()[0][0])
        response = response["choices"][0]["messages"]
        st.write(response)

        message = {"role": "assistant", "content": response[:50].replace("'", "''")}
        st.session_state.messages.append(message)

        if sql_match := re.search(r"```sql\n(.*)\n```", response, re.DOTALL):
            st.divider()
            st.write("Generated query:")
            query = sql_match.group(1)
            st.code(query)
            st.dataframe(session.sql(query).to_pandas(), use_container_width=True)
        else:
            st.error("No query!")