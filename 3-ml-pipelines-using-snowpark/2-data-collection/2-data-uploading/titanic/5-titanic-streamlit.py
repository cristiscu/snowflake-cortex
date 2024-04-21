# run from a terminal in VSCode: streamlit run 6-streamlit-web-app.py

# connect to Snowflake through Snowpark
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

import streamlit as st
st.title("Upload Titanic Data")

if st.sidebar.button("🗂️ Re-create Stage", use_container_width=True):
    query = "CREATE OR REPLACE STAGE TEST.PUBLIC.INT_STAGE"
    session.sql(query).collect()
    st.write("Stage INT_STAGE created.")

if st.sidebar.button("🗃️ Upload File in Stage", use_container_width=True):
    # this will not work in a Streamlit in Snowflake app!
    session.file.put(
        "..\..\..\..\.spool\\titanic.csv",
        "TEST.PUBLIC.INT_STAGE",
        auto_compress=False, overwrite=True)
    st.write("File titanic.csv uploaded in stage.")

if st.sidebar.button("🗓️ Re-create Table", use_container_width=True):
    query = """
    CREATE OR REPLACE TABLE TEST.PUBLIC.TITANIC (
        "PassengerId" INT,
        "Survived" INT,
        "Pclass" INT,
        "Name" VARCHAR,
        "Sex" VARCHAR,
        "Age" INT,
        "SibSp" INT,
        "Parch" INT,
        "Ticket" VARCHAR,
        "Fare" FLOAT,
        "Cabin" VARCHAR,
        "Embarked" VARCHAR)
    """
    session.sql(query).collect()
    st.write("Table TITANIC created.")

if st.sidebar.button("🔝 Copy Data into Table", use_container_width=True):
    query = """
    COPY INTO TEST.PUBLIC.TITANIC
    FROM @TEST.PUBLIC.INT_STAGE/titanic.csv
    FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    """
    session.sql(query).collect()
    st.write("File data copied into the TITANIC table.")

if st.sidebar.button("❓ Query Data from Table", use_container_width=True):
    query = """
    SELECT *
    FROM TEST.PUBLIC.TITANIC
    LIMIT 10
    """
    df = session.sql(query).to_pandas()
    st.dataframe(df, use_container_width=True)
    st.write("First 100 rows from TITANIC table.")
