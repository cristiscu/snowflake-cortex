# see https://github.com/Snowflake-Labs/sfguide-snowpark-scikit-learn/blob/main/2_data_exploration_transformation.ipynb
import sys
import numpy as np
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

# Connect to Snowflake w/ a Snowpark session and SNOWSQL config file data
pars = SnowflakeLoginOptions("test_conn")
pars["session_parameters"] = { 'QUERY_TAG': 'snowpark_queries' }
session = Session.builder.configs(pars).create()
# session.query_tag = 'snowpark_queries'
#print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())

# Connect to the HOUSING table (but nothing loaded!)
df = session.table('HOUSING')
size = np.round(sys.getsizeof(df) / (1024.0**2), 2)
print(f'Memory: {size} MB')
#df.show()
#df.queries

# Add calculated column and select some columns
df = df.with_column('BEDROOM_RATIO', F.col('TOTAL_BEDROOMS') / F.col('TOTAL_ROOMS'))
df = df.select('HOUSING_MEDIAN_AGE', 'TOTAL_ROOMS',
    'TOTAL_BEDROOMS', 'HOUSEHOLDS', 'OCEAN_PROXIMITY',
    'BEDROOM_RATIO')
#df.show()

# Drop calculated column
df = df.drop('BEDROOM_RATIO')
#df.show()

# Filter data
df = df.filter(F.col('OCEAN_PROXIMITY').in_(['INLAND','ISLAND', 'NEAR BAY']))
#df.show()
#df.queries

# Aggregate & sort data
df = df.group_by(['OCEAN_PROXIMITY']).agg([F.avg('HOUSEHOLDS').as_('AVG_HOUSEHOLDS')])
df = df.sort(F.col('AVG_HOUSEHOLDS').asc())
#df.show()

# Save Snowpark DataFrame into a table
df.write.mode("overwrite").save_as_table("HOUSING_SNOWPARK")
