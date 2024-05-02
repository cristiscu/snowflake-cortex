from snowflake.ml.modeling.xgboost import XGBClassifier

XGBClassifier(
    input_cols=["F"],
    label_cols=["L"],
    output_cols=["P"]).fit(train_df)
