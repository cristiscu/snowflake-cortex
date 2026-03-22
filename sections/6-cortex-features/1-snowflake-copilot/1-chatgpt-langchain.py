# Questions:
# ==========
# Show me the total number of entries in the first table
# Select top 10 customers from Canada with highest sum of C_ACCTBAL value, in descending order
# Show me the total of Customers per Nation, in ascending order
# Show me a query that lists totals for extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in ascending order of RETURNFLAG and LINESTATUS. A count of the number of line items in each group is included

import os
import streamlit as st
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from langchain_community.utilities import SQLDatabase
from langchain_openai import OpenAI
from langchain.chains import create_sql_query_chain

@st.cache_resource(show_spinner="Connecting...")
def getSession():
    pars = SnowflakeLoginOptions("test_conn")
    pars["database"] = "SNOWFLAKE_SAMPLE_DATA"
    pars["schema"] = "TPCH_SF1"
    session = Session.builder.configs(pars).create()

    url = (f"snowflake://{pars['user']}:{pars['password']}@{pars['account']}"
        + f"/{pars['database']}/{pars['schema']}"
        + f"?warehouse={pars['warehouse']}&role={pars['role']}")
    db = SQLDatabase.from_uri(url)

    openai_key = os.environ["OPENAI_API_KEY"]
    llm = OpenAI(openai_api_key=openai_key)
    chain = create_sql_query_chain(llm, db)
    return session, db, chain


st.title("SQL Query Generator")
st.write("Returns and runs queries from questions in natural language.")

session, db, chain = getSession()

question = st.sidebar.text_area("Ask a question:",
    value="Show me the total number of entries in the first table")
sql = chain.invoke({"question": question}).rstrip(';')

tabQuery, tabData, tabLog = st.tabs(["Query", "Data", "Log"])
tabQuery.code(sql, language="sql")
tabData.dataframe(session.sql(sql))
tabLog.code(db.table_info, language="sql")
