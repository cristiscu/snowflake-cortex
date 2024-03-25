use schema openai_db.public;

select 'Chile' as country,
    openai('President of ' || country) as answer;

select
   n_name as country,
   openai('Continent of ' || n_name || ' as one single word and nothing else') as continent
from snowflake_sample_data.tpch_sf1.nation
limit 5;
