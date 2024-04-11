# see https://github.com/Snowflake-Labs/sfguide-snowpark-scikit-learn/blob/main/1_snowpark_housing_data_ingest.ipynb
# download remote TAR file + decompress into local CSV

REMOTE_PATH = "https://raw.githubusercontent.com/ageron/handson-ml2/master/datasets/housing"
LOCAL_PATH = "../../.spool/datasets"

import os, tarfile, urllib.request
urllib.request.urlretrieve(
    f"{REMOTE_PATH}/housing.tgz",
    f"{LOCAL_PATH}/housing.tgz")

with tarfile.open(f"{LOCAL_PATH}/housing.tgz") as file:
    file.extractall(path=LOCAL_PATH, filter="fully_trusted")

os.remove(f"{LOCAL_PATH}/housing.tgz")