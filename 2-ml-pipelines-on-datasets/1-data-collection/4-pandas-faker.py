import random
import pandas as pd
from faker import Faker

f = Faker()
output = [[f.name(), f.country(), f.city(), f.state(), random.randrange(100, 10000)]
    for _ in range(10000)]

df = pd.DataFrame(output)
print(df.head())