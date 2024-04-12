CREATE OR REPLACE DATABASE openai_db;

CREATE OR REPLACE SECRET openai_key
    TYPE = GENERIC_STRING
    SECRET_STRING = '<your_OpenAI_key>';

CREATE OR REPLACE NETWORK RULE openai_nr
   TYPE = HOST_PORT
   MODE = EGRESS
   VALUE_LIST = ('api.openai.com');

-- not for trial accounts!
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION openai_eai 
   ALLOWED_NETWORK_RULES = (openai_nr)
   ALLOWED_AUTHENTICATION_SECRETS = (openai_key) 
   ENABLED = TRUE;

CREATE OR REPLACE function openai(prompt text)
  RETURNS text
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (openai_eai)
  SECRETS = ('cred'=openai_key)
  HANDLER = 'handler'
AS $$
import snowflake.snowpark as snowpark
import _snowflake, requests, json

def handler(prompt):
    key = _snowflake.get_generic_secret_string('cred');
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={'Authorization': f'Bearer {key}'},
        json={"model": "gpt-4-turbo-2024-04-09",
              "messages": [{"role": "user", "content": prompt}],
              "temperature": 0.7}
    ).json()
    return r["choices"][0]["message"]["content"]
$$;

select 'Chile' as country,
    openai('President of ' || country) as answer;

select
   n_name as country,
   openai('Continent of ' || n_name || ' as one single word and nothing else') as continent
from snowflake_sample_data.tpch_sf1.nation
limit 5;
