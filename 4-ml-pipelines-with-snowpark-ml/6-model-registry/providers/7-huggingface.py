# see https://docs.snowflake.com/en/developer-guide/snowpark-ml/snowpark-ml-mlops-model-registry#hugging-face-pipeline

import transformers
import pandas as pd
from model_utils import get_registry

finbert_model = transformers.pipeline(
    task="text-classification",
    model="ProsusAI/finbert",
    top_k=2)

print("Registering the model...")
registry = get_registry()
model_ref = registry.log_model(
    finbert_model,
    model_name="finbert",
    conda_dependencies=["tokenizers", "transformers"],
    version_name="v1")

print("Making a prediction...")
df = model_ref.run(pd.DataFrame([
    ["I have a problem with my Snowflake that needs to be resolved asap!!", ""],
    ["I would like to have udon for today's dinner.", ""]]))
print(df)