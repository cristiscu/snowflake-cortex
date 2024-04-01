import random
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from faker import Faker

def main(session: snowpark.Session):
    f = Faker()
    output = [[f.name(), f.country(), f.city(), f.state(), random.randrange(100, 10000)]
        for _ in range(10000)]

    schema = StructType([ 
        StructField("NAME", StringType(), False),  
        StructField("COUNTRY", StringType(), False), 
        StructField("CITY", StringType(), False),  
        StructField("STATE", StringType(), False),  
        StructField("SALES", IntegerType(), False)])
    df = session.create_dataframe(output, schema)
    df.write.mode("overwrite").save_as_table("CUSTOMERS_FAKE")
    df.show()
    return df