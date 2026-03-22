import snowflake.snowpark as snowpark
from sklearn.preprocessing import OrdinalEncoder, MinMaxScaler

# 12 min for 100M rows w/ SO-Medium
def main(session: snowpark.Session): 
    X = session.table("REGRESSION_DATASET").to_pandas()
    X[["C1O", "C2O"]] = OrdinalEncoder().fit_transform(X[["C1", "C2"]])
    X[["N1FO", "N2FO", "C1FO", "C2FO"]] = MinMaxScaler().fit_transform(X[["N1", "N2", "C1O", "C2O"]])
    return session.create_dataframe(X)
