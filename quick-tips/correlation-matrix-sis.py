# copy all this into a Streamlit in Snowflake app

import streamlit as st
from snowflake.snowpark.context import get_active_session
import matplotlib.pyplot as plt
import seaborn as sns

df = get_active_session().table('housing').to_pandas()
st.dataframe(df)

fig, ax = plt.subplots()
sns.heatmap(df.corr(numeric_only=True), ax=ax)
st.write(fig)