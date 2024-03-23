-- see https://www.snowflake.com/blog/ml-powered-functions-improve-speed-quality/

// Add series_colname to predict sales by category. 
create snowflake.ml.forecast revenue_projector_by_store(
    input_data => SYSTEM$REFERENCE('VIEW', 'daily_revenue_v'),
    timestamp_colname => 'ts',
    target_colname => 'revenue',
    series_colname => 'store_id'
);

// The model is now ready for prediction.
call revenue_projector_by_store!forecast(
    forecasting_periods => 30, // Predict sales for one month.
);