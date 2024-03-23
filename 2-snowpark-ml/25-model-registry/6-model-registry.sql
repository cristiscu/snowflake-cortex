-- see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry

SELECT <model_name>!<method_name>(...) FROM <table_name>;

WITH <model_version_alias> AS MODEL <model_name> VERSION <version>
    SELECT <model_version_alias>!<method_name>(...) FROM <table_name>;