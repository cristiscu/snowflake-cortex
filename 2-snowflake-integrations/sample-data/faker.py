import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType
from faker import Faker

def main(session: snowpark.Session):
    f = Faker()
    output = [[f.name(), f.address(), f.city(), f.state(), f.email()]
        for _ in range(10000)]

    schema = StructType([ 
        StructField("NAME", StringType(), False),  
        StructField("ADDRESS", StringType(), False), 
        StructField("CITY", StringType(), False),  
        StructField("STATE", StringType(), False),  
        StructField("EMAIL", StringType(), False)])
    df = session.create_dataframe(output, schema)
    df.write.mode("overwrite").save_as_table("CUSTOMERS_FAKE")
    df.show()
    return df