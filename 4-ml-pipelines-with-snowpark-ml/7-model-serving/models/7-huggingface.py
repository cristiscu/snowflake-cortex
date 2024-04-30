# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#hugging-face-pipeline

import pandas as pd
from model_utils import get_model

model_ref = get_model("finbert")
model_ref.run(pd.DataFrame([
    ["I have a problem with my Snowflake that needs to be resolved asap!!", ""],
    ["I would like to have udon for today's dinner.", ""]]))