-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-documents
-- manually create a test.public.stage9 stage (if not there)
-- then upload all PDF and DOCX document files from the .stage9/docs subfolder into the stage
use test.public;

LIST @stage9/docs;

-- parse doc (in default LAYOUT mode)
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@stage9', 'docs/doc1.pdf')) AS ex;

SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@stage9', 'docs/doc1.pdf'),
    {'mode': 'LAYOUT', 'page_split': true}) AS ex;

SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@stage9', 'docs/doc1.pdf'),
    {'mode': 'LAYOUT', 'page_filter': [{'start': 0, 'end': 1}]}) AS ex;

-- parse doc (in OCR mode)
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@stage9', 'docs/doc1.pdf'),
    {'mode': 'OCR'}) AS ex;

-- extract images from doc
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/image-extraction
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@stage9', 'docs/doc1.pdf'),
    {'mode': 'LAYOUT', 'extract_images': true}) AS ex;

-- ===========================================================
-- classify docs (through temporary table)
SELECT TO_FILE('@stage9', RELATIVE_PATH) AS doc
FROM DIRECTORY(@stage9)
WHERE RELATIVE_PATH LIKE 'docs/%';

CREATE OR REPLACE TEMPORARY TABLE docs AS
(SELECT TO_FILE('@stage9', RELATIVE_PATH) AS doc
FROM DIRECTORY(@stage9)
WHERE RELATIVE_PATH LIKE 'docs/%');

SELECT AI_CLASSIFY(content,
    ['health', 'fitness', 'travel'],
    {'output_mode': 'multi'}) as classif
FROM (SELECT TO_VARCHAR(AI_PARSE_DOCUMENT(doc)) AS content
    FROM docs);

-- ===========================================================
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-complete-document-intelligence

-- analyze document data
SELECT AI_COMPLETE(
    MODEL => 'claude-4-opus',
    PROMPT => PROMPT(
        'Compare the two Times Square images in {0}', 
        TO_FILE('@stage9', 'docs/doc1.pdf')));

-- summarize document text
SELECT AI_COMPLETE(
    MODEL => 'gemini-3-pro',
    PROMPT => PROMPT(
        'Summarize content of {0}', 
        TO_FILE('@stage9', 'docs/doc1.pdf')));

-- ===========================================================
-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/document-extraction

-- extract info from document form image
SELECT AI_EXTRACT(
    file => TO_FILE('@stage9', 'docs/doc1.pdf'),
    responseFormat => [
        ['seller_name', 'What is the seller name?'],
        ['address', 'What is the offer expiration date?']]) AS extract;

-- extract infor from document table
SELECT AI_EXTRACT(
    file => TO_FILE('@stage9', 'docs/doc1.pdf'),
    responseFormat => {
        'schema': {
            'type': 'object',
            'properties': {
                'my_table': {
                    'type': 'object',
                    'column_ordering': ['description', 'countries', 'lags', 'z', 'z_approx'],
                    'properties': {
                        'description': { 'description': 'Description', 'type': 'array' },
                        'countries': { 'description': 'Countries', 'type': 'array' },
                        'lags': { 'description': 'Lags', 'type': 'array' },
                        'z': { 'description': 'Z', 'type': 'array' },
                        'z_approx': { 'description': 'Z approx.', 'type': array }
                    }
                }
            }
        }
    }) AS extract;