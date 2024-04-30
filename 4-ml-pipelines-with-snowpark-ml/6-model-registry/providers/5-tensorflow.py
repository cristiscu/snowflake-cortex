# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#tensorflow

import tensorflow as tf
import numpy as np
from model_utils import get_registry

class KerasModel(tf.keras.Model):
    def __init__(self, n_hidden: int, n_out: int) -> None:
        super().__init__()
        self.fc_1 = tf.keras.layers.Dense(n_hidden, activation="relu")
        self.fc_2 = tf.keras.layers.Dense(n_out, activation="sigmoid")

    def call(self, tensor: tf.Tensor) -> tf.Tensor:
        input = tensor
        x = self.fc_1(input)
        x = self.fc_2(x)
        return x

n_input, n_hidden, n_out, batch_size, learning_rate = 10, 15, 1, 100, 0.01
dtype = tf.float32
x = np.random.rand(batch_size, n_input)
data_x = tf.convert_to_tensor(x, dtype=dtype)
raw_data_y = tf.random.uniform((batch_size, 1))
raw_data_y = tf.where(raw_data_y > 0.5,
    tf.ones_like(raw_data_y),
    tf.zeros_like(raw_data_y))
data_y = tf.cast(raw_data_y, dtype=dtype)

model = KerasModel(n_hidden, n_out)
model.compile(
    optimizer=tf.keras.optimizers.SGD(learning_rate=learning_rate),
    loss=tf.keras.losses.MeanSquaredError())
model.fit(data_x, data_y, batch_size=batch_size, epochs=100)

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    model,
    model_name="tfModel",
    version_name="v1",
    conda_dependencies=["tensorflow"],
    sample_input_data=[data_x])

print("Making a prediction...")
df = model_ref.run([data_x])
print(df)