from snowflake.ml.modeling.preprocessing import MinMaxScaler, OrdinalEncoder
from snowflake.ml.modeling.pipeline import Pipeline
import snowflake.snowpark as snowpark

# 2.15 min for 100M rows w/ SO-Medium
def main(session: snowpark.Session): 
    df = session.table("REGRESSION_DATASET")
    
    pipe = Pipeline(steps=[
        ("encoder", OrdinalEncoder(
            input_cols=["C1", "C2"],
            output_cols=["C1O", "C2O"])),
        ("scaler", MinMaxScaler(
            input_cols=["N1", "N2", "C1O", "C2O"],
            output_cols=["N1FO", "N2FO", "C1FO", "C2FO"]))])
    
    pipe.fit(df)
    df = pipe.transform(df)
    return df
