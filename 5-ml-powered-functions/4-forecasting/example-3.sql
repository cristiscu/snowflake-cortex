-- see https://www.snowflake.com/blog/ml-powered-functions-improve-speed-quality/

// This shows training & prediction for revenues in daily_revenue_v
create snowflake.ml.forecast revenue_projector(
    input_data => SYSTEM$REFERENCE('VIEW', 'daily_revenue_v'),
    timestamp_colname => 'ts',
    target_colname => 'revenue'
);

// The model is now ready for prediction.
call revenue_projector!forecast(
    forecasting_periods => 30, // how far out to project!
    config_object => {'prediction_interval': 0.9} // optional range of values with this probability
); 

