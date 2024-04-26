## Model Training in Snowpark

```
@sproc(name="train_model", replace=True,
    is_permanent=True, stage_location="@mystage",
    packages=["snowflake-snowpark-python"])
def train_model(session: Session, x: int) -> int:
    return XGBClassifier()....

train_model(...)
```

## Model Training in Snowpark ML

```
def XGBClassifier:

    @sproc(name="train_model", replace=True,
        is_permanent=True, stage_location="@mystage",
        packages=["snowflake-snowpark-python"])
    def train_model(session: Session, x: int) -> int:
        return XGBClassifier()....

    train_model(...)

XGBClassifier()...
```