# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#tensorflow

import tensorflow as tf
import numpy as np
from model_utils import get_model

data_x = tf.convert_to_tensor(np.random.rand(100, 10), dtype=tf.float32)

print("Making a prediction...")
model_ref = get_model("tfModel")
df = model_ref.run([data_x])
print(df)