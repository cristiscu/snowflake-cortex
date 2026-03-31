-- Snowflake Cortex Fine-tuning: SNOWFLAKE.CORTEX.FINETUNE Function
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-finetuning
use test.public;

-- upload the .spool/support_tickets.csv file data into the new table below
CREATE OR REPLACE TABLE support_tickets(
    ticket_id VARCHAR(60),
    customer_name VARCHAR(60),
    customer_email VARCHAR(60),
    service_type VARCHAR(60),
    request VARCHAR,
    contact_preference VARCHAR(60));
TABLE support_tickets;

-- create another related table with an additional completion column, with data from Mistral Large
set prompt = $$ You are an agent that helps organize the requests that come to your support team.

These are the possible types of request categories:

Slow performance
Product Info
Account Management

Return only the request category.

$$;

CREATE OR REPLACE TABLE support_tickets_large AS
select ticket_id,
    request as prompt,
    trim(snowflake.cortex.complete(
        'mistral-large', concat($prompt, request)), '\n') as completion
from support_tickets;
TABLE support_tickets_large;

-- check similar data from smaller Mistral 7B
CREATE OR REPLACE TABLE support_tickets_small AS
select ticket_id,
    request as prompt,
    trim(snowflake.cortex.complete(
        'mistral-7b', concat($prompt, request)), '\n') as completion
from support_tickets;
TABLE support_tickets_small;

-- separate training dataset = first 160 (out of 200)
CREATE OR REPLACE VIEW support_tickets_train AS
SELECT * FROM support_tickets_large
LIMIT 160;

-- separate test/validation dataset = last 40 (out of 200)
CREATE OR REPLACE VIEW support_tickets_test AS
SELECT * FROM support_tickets_large
LIMIT 40 OFFSET 160;

-- fine-tune with smaller Mistral 7B (try in parallel in the Playground!)
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE', 'support_tickets_model', 'mistral-7b',
    'SELECT prompt, completion FROM support_tickets_train',
    'SELECT prompt, completion FROM support_tickets_test') as job_id;

SELECT SNOWFLAKE.CORTEX.FINETUNE('SHOW');
SELECT SNOWFLAKE.CORTEX.FINETUNE('DESCRIBE', '<your-job-id>');
-- SELECT SNOWFLAKE.CORTEX.FINETUNE('CANCEL', '<your-job-id>');

-- re-check completion data, but from the tuned model
CREATE OR REPLACE TABLE support_tickets_tuned AS
select ticket_id,
    request as prompt,
    trim(snowflake.cortex.complete(
        'support_tickets_model', concat($prompt, request)), '\n') as completion
from support_tickets;
TABLE support_tickets_tuned;

SELECT st.ticket_id, st.request as prompt,
    stl.completion as large, sts.completion as small, stt.completion as tuned
FROM support_tickets st
JOIN support_tickets_large stl ON st.ticket_id=stl.ticket_id
JOIN support_tickets_small sts ON st.ticket_id=sts.ticket_id
JOIN support_tickets_tuned stt ON st.ticket_id=stt.ticket_id;

-- cleanup
-- DROP MODEL support_tickets_tuned;
SELECT SNOWFLAKE.CORTEX.FINETUNE('CANCEL', '<your-job-id>');

