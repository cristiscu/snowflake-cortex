# Snowflake Cortex

All demo source code for my Udemy course **Snowflake Cortex Masterclass 2024 Hands-On!**.  

Refer to individual sections for local documentation.  

## Setup Instructions

1) [Download and install **Python**](https://www.python.org/downloads/).

2) From a Terminal window in VSCode, create and switch to a *virtual environment*:

```
python -m venv venv
venv/scripts/activate
```

Your Terminal prompt should now start with *(venv)*.

3) Upgrade **pip**:

```
python -m pip install --upgrade pip
```

4) Install dependencies for the whole repository, from **requirements.txt**:

```
pip install -r requirements.txt
```

This may take a while. It will install Snowpark, Snowpark ML with all dependencies, OpenAI API, Streamlit and other requiered or optional libraries for all our projects.

5) [Install **SnowSQL**](https://docs.snowflake.com/en/user-guide/snowsql-install-config).

6) In your Windows user directory, go to the **.snowsql/config** file and add a section with your Snowflake connection parameters:

```
[connections.test_conn]
accountname = <your_Snowflake_locator>
username = <your_admin_name>
password = <your_pwd>
database = test
schema = public
warehouse = compute_wh
```

To keep it simple, we'll try to use almost everywhere only the following simple Python code snippet, to connect to Snowflake through a Snowpark session:

```
# connect to your Snowflake account
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
session = Session.builder.configs(SnowflakeLoginOptions("test_conn")).create()
```

7) In VSCode, install the [**Snowflake Extension**](https://docs.snowflake.com/en/user-guide/vscode-ext), and manually connect to your Snowflake account.

8) In your free trial Snowflake account, create a **TEST** database.

9) Test your Snowflake connection, running from the Terminal window:

```
python test.py
```

This should show help on the *OneHotEncoder* class from Snowpark ML. Exit with CTRL_C.