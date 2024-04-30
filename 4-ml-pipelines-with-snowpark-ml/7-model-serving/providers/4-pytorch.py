# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#pytorch

import torch
import numpy as np
from model_utils import get_model

data_x = (torch
    .from_numpy(np.random.rand(100, 10))
    .to(dtype=torch.float32))

print("Making a prediction...")
model_ref = get_model("torchModel")
df = model_ref.run([data_x])
print(df)