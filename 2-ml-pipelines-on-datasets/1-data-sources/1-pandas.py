# see https://github.com/Snowflake-Labs/sfguide-snowpark-scikit-learn/blob/main/1_snowpark_housing_data_ingest.ipynb
# execute to download CSV file
# manually import into new TEST.PUBLIC.HOUSING table
# SELECT * FROM housing --> 20.6K rows, 9+1 numeric/categ columns
# see column profiles (distribution, min/max etc)

import os, tarfile
import urllib.request
import pandas as pd 

REMOTE_PATH = "https://raw.githubusercontent.com/ageron/handson-ml2/master/datasets/housing/"
LOCAL_PATH = "../../.datasets/"
TAR_FILE = "housing.tgz"
CSV_FILE = "housing.csv"

# download remote TAR file
urllib.request.urlretrieve(
    REMOTE_PATH + TAR_FILE, LOCAL_PATH + TAR_FILE)

# decompress TAR file into CSV
with tarfile.open(LOCAL_PATH + TAR_FILE) as file:
    file.extractall(path=LOCAL_PATH, filter="fully_trusted")
os.remove(LOCAL_PATH + TAR_FILE)

# load and display CSV into a Pandas data frame
df  = pd.read_csv(LOCAL_PATH + CSV_FILE) 
print(df.head())