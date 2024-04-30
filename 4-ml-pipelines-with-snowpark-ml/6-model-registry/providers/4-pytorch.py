# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#pytorch

import torch
import numpy as np
from model_utils import get_registry

class TorchModel(torch.nn.Module):
    def __init__(self, n_input: int, n_hidden: int, n_out: int,
        dtype: torch.dtype = torch.float32) -> None:
        super().__init__()
        self.model = torch.nn.Sequential(
            torch.nn.Linear(n_input, n_hidden, dtype=dtype),
            torch.nn.ReLU(),
            torch.nn.Linear(n_hidden, n_out, dtype=dtype),
            torch.nn.Sigmoid())

    def forward(self, tensor: torch.Tensor) -> torch.Tensor:
        return self.model(tensor)

n_input, n_hidden, n_out, batch_size, learning_rate = 10, 15, 1, 100, 0.01
dtype = torch.float32
x = np.random.rand(batch_size, n_input)
data_x = torch.from_numpy(x).to(dtype=dtype)
data_y = (torch.rand(size=(batch_size, 1)) < 0.5).to(dtype=dtype)

model = TorchModel(n_input, n_hidden, n_out, dtype=dtype)
loss_function = torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)

for _epoch in range(100):
    pred_y = model.forward(data_x)
    loss = loss_function(pred_y, data_y)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    model,
    model_name="torchModel",
    version_name="v1",
    conda_dependencies=["pytorch", "torch"],
    sample_input_data=[data_x])

print("Making a prediction...")
df = model_ref.run([data_x])
print(df)