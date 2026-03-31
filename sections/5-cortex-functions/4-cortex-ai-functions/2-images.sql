-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-images
-- manually create a test.public.stage9 stage (if not there)
-- then upload all JPG image files from the .stage9/images subfolder into the stage
use test.public;

LIST @stage9/images;

-- analyze image, and provide a summary
SELECT AI_COMPLETE('claude-3-5-sonnet',
    'Summarize the insights from this Times Square image in 100 words',
    TO_FILE('@stage9', 'images/times-square-3.jpg'));

-- compare two images
SELECT AI_COMPLETE('claude-3-5-sonnet',
    PROMPT('Compare this image {0} to this image {1} and describe the ideal audience for each in two concise bullets no longer than 10 words',
    TO_FILE('@stage9', 'images/times-square-2.jpg'),
    TO_FILE('@stage9', 'images/times-square-5.jpg')));

-- classify image, in single category
SELECT AI_CLASSIFY(
    TO_FILE('@stage9', 'images/times-square-5.jpg'),
    ['People', 'Electronics', 'Crowded', 'Nature']) AS classif;

-- classify image, with multiple categories
SELECT AI_CLASSIFY(
    TO_FILE('@stage9', 'images/times-square-5.jpg'),
    ['People', 'Electronics', 'Crowded', 'Nature'],
    {'output_mode': 'multi'}) AS classif;

-- ===========================================================
-- detect how similar two images are
WITH embeds AS (
    SELECT
        AI_EMBED('voyage-multimodal-3', TO_FILE('@stage9', 'images/times-square-3.jpg')) as e1,
        AI_EMBED('voyage-multimodal-3', TO_FILE('@stage9', 'images/times-square-4.jpg')) as e2)
SELECT VECTOR_COSINE_SIMILARITY(e1, e2) as similarity
FROM embeds;

-- find all images similar to another image
SELECT TO_FILE('@stage9', RELATIVE_PATH) as files,
    AI_SIMILARITY(TO_FILE('@stage9', 'images/times-square-1.jpg'), files) as sim
FROM DIRECTORY(@stage9)
WHERE RELATIVE_PATH LIKE 'images/%.jpg';