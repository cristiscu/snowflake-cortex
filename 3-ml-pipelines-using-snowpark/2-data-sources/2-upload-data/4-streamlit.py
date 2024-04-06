
import streamlit as st
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()

st.title("Magic Gamma Telescope Data")

if st.sidebar.button("🗓️ Create Table", use_container_width=True):
    query = """
    CREATE OR REPLACE TABLE TEST.PUBLIC.Gamma_Telescope(
        F_LENGTH FLOAT,
        F_WIDTH FLOAT,
        F_SIZE FLOAT,
        F_CONC FLOAT,
        F_CONC1 FLOAT,
        F_ASYM FLOAT,
        F_M3_LONG FLOAT,
        F_M3_TRANS FLOAT,
        F_ALPHA FLOAT,
        F_DIST FLOAT,
        CLASS VARCHAR(10))
    """
    session.sql(query).collect()
    st.write("Table Gamma_Telescope created.")

if st.sidebar.button("🗂️ Create Stage", use_container_width=True):
    query = "CREATE OR REPLACE STAGE TEST.PUBLIC.Gamma_Stage"
    session.sql(query).collect()
    st.write("Stage Gamma_Stage created.")

if st.sidebar.button("🗃️ Upload File", use_container_width=True):
    session.file.put(
        "..\..\..\.spool\datasets\gamma-telescope.csv",
        "TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv",
        auto_compress=False, overwrite=True)
    st.write("File gamma-telescope.csv uploaded in stage.")

if st.sidebar.button("🗂️ List Files", use_container_width=True):
    query = "LIST @TEST.PUBLIC.Gamma_Stage"
    df = session.sql(query).collect()
    st.dataframe(df, use_container_width=True)
    st.write("Listed all files in stage.")

if st.sidebar.button("🗃️ Query File", use_container_width=True):
    query = """
    SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
    FROM @TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv
    LIMIT 100
    """
    df = session.sql(query).collect()
    st.dataframe(df, use_container_width=True)
    st.write("First 100 rows from file displayed here.")

if st.sidebar.button("🔝 Copy Data", use_container_width=True):
    query = """
    COPY INTO TEST.PUBLIC.Gamma_Telescope
    FROM @TEST.PUBLIC.Gamma_Stage/gamma-telescope.csv
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=0);
    """
    session.sql(query).collect()
    st.write("File data copied into the Gamma_Telescope table.")

if st.sidebar.button("❓ Query Data", use_container_width=True):
    query = "SELECT * FROM TEST.PUBLIC.Gamma_Telescope LIMIT 100"
    df = session.sql(query).to_pandas()
    st.dataframe(df, use_container_width=True)
    st.write("First 100 rows displayed here.")
