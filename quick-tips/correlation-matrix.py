import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("../.spool/housing.csv")
st.dataframe(df)

fig, ax = plt.subplots()
sns.heatmap(df.corr(numeric_only=True), ax=ax)
st.write(fig)