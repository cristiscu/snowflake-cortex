# see https://github.com/Snowflake-Labs/sfguide-snowpark-scikit-learn/blob/main/1_snowpark_housing_data_ingest.ipynb

import os, tarfile, urllib.request

REMOTE = "https://raw.githubusercontent.com/ageron/handson-ml2/master/datasets/housing"
LOCAL = "../../../.spool"

# download remote TAR file
urllib.request.urlretrieve(f"{REMOTE}/housing.tgz", f"{LOCAL}/housing.tgz")

# decompress into local CSV
with tarfile.open(f"{LOCAL}/housing.tgz") as file:
    file.extractall(path=LOCAL)

# delete local archive
os.remove(f"{LOCAL}/housing.tgz")