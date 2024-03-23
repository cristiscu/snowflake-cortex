import os
import streamlit as st
from langchain_community.utilities import SQLDatabase
from langchain_openai import OpenAI
from langchain.chains import create_sql_query_chain

@st.cache_resource(show_spinner="Connecting...")
def getSession():
    from snowflake.snowpark import Session
    from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
    pars = SnowflakeLoginOptions("test_conn")
    pars["database"] = "SNOWFLAKE_SAMPLE_DATA"
    pars["schema"] = "TPCH_SF1"
    session = Session.builder.configs(pars).create()

    url = (f"snowflake://{pars['username']}:{pars['password']}@{pars['accountname']}"
        + f"/{pars['database']}/{pars['schema']}"
        + f"?warehouse={pars['warehouse']}&role={pars['role']}")
    db = SQLDatabase.from_uri(url)

    openai_key = os.environ["OPENAI_API_KEY"]
    llm = OpenAI(openai_api_key=openai_key)
    chain = create_sql_query_chain(llm, db)
    return session, db, chain

st.title("LangChain SQL Generator")
st.write("Returns and runs queries from questions in natural language.")

session, db, chain = getSession()

question = st.sidebar.text_area("Ask a question:",
    value="show me the total number of entries in the first table")
sql = chain.invoke({"question": question})

tabQuery, tabData, tabLog = st.tabs(["Query", "Data", "Log"])
tabQuery.code(sql, language="sql")
tabData.dataframe(session.sql(sql))
tabLog.code(db.table_info, language="sql")
