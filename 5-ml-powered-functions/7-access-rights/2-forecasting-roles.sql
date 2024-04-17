-- see https://docs.snowflake.com/en/user-guide/ml-powered-forecasting#granting-privileges-to-create-forecast-objects
-- see https://docs.snowflake.com/en/user-guide/ml-powered-anomaly-detection#granting-privileges-to-create-anomaly-detection-objects

USE ROLE admin;
SHOW ROLES IN CLASS ANOMALY_DETECTION;
SHOW GRANTS TO ANOMALY_DETECTION ROLE db.schema.model!USER;

GRANT ANOMALY_DETECTION ROLE db.schema.model!USER TO ROLE role;
GRANT CREATE ANOMALY_DETECTION ON SCHEMA db.schema TO ROLE admin;
REVOKE CREATE ANOMALY_DETECTION ON SCHEMA db.schema FROM ROLE admin;
