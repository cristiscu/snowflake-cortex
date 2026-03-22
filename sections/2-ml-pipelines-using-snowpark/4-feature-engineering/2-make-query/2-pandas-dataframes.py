# see https://github.com/Snowflake-Labs/sfguide-snowpark-scikit-learn/blob/main/2_data_exploration_transformation.ipynb
import os, sys, configparser
import numpy as np
#from IPython.display import display
import snowflake.connector

# Connect to Snowflake w/ the Python Connector and SNOWSQL config file data
parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
section = "connections.test_conn"
conn = snowflake.connector.connect(
    account=parser.get(section, "accountname"),
    user=parser.get(section, "username"),
    password=parser.get(section, "password"),
    database=parser.get(section, "database"),
    schema=parser.get(section, "schema"),
    session_parameters={ 'QUERY_TAG': 'pandas_queries' })

# Load all HOUSING table records in memory
query = "SELECT * FROM test.public.housing"
df = conn.cursor().execute(query).fetch_pandas_all()
size = np.round(sys.getsizeof(df) / (1024.0**2), 2)
print(f'Memory: {size} MB')
#display(df)

# Add calculated column and select some columns
df['BEDROOM_RATIO'] = df['TOTAL_BEDROOMS'] / df['TOTAL_ROOMS']
df = df[['HOUSING_MEDIAN_AGE', 'TOTAL_ROOMS', 'TOTAL_BEDROOMS', 'HOUSEHOLDS', 'OCEAN_PROXIMITY', 'BEDROOM_RATIO']]
#display(df)

# Drop calculated column
df = df.drop(columns=['BEDROOM_RATIO'])
#display(df)

# Filter data
#df = (df[(df['OCEAN_PROXIMITY'] == 'INLAND')
#    | (df['OCEAN_PROXIMITY'] == 'ISLAND')
#    | (df['OCEAN_PROXIMITY'] == 'NEAR BAY')])
filter = df['OCEAN_PROXIMITY'].isin(['INLAND', 'ISLAND', 'NEAR BAY'])
df = df[filter]
#display(df)

# Aggregate & sort data
df = df.groupby(['OCEAN_PROXIMITY'])['HOUSEHOLDS'].agg(AVG_HOUSEHOLDS='mean')
df = df.reset_index(drop=True)
df = df.sort_values('AVG_HOUSEHOLDS')
#display(df)

# Dump pandas DataFrame into a Snowflake table
# see https://community.snowflake.com/s/article/How-to-use-Write-Pandas-method-to-create-a-table-when-it-does-not-exist
from snowflake.connector.pandas_tools import write_pandas
write_pandas(conn, df, 'HOUSING_PANDAS', auto_create_table=True, overwrite=True)
