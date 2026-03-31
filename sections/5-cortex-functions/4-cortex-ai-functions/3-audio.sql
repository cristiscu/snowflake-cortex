-- see https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-audio
-- manually create a test.public.stage9 stage (if not there)
-- then upload all WAV audio files from the .stage9/audio subfolder into the stage
use test.public;

LIST @stage9/audio;

-- generate transcript from audio (English)
SELECT AI_TRANSCRIBE(
    TO_FILE('@stage9', 'audio/audio-en.wav'));

-- generate transcript from audio (French)
SELECT AI_TRANSCRIBE(
    TO_FILE('@stage9', 'audio/audio-fr.wav'));

-- further translate French response in English
SELECT AI_TRANSLATE($1, 'fr', 'en')
FROM (SELECT TO_VARCHAR(AI_TRANSCRIBE(
    TO_FILE('@stage9', 'audio/audio-fr.wav'))));

-- see word-level segmentation with timestamps
SELECT AI_TRANSCRIBE(
    TO_FILE('@stage9', 'audio/audio-en.wav'),
    {'timestamp_granularity': 'word'});

-- recognize different speakers
SELECT AI_TRANSCRIBE(
    TO_FILE('@stage9', 'audio/audio-en.wav'),
    {'timestamp_granularity': 'speaker'});

-- ===========================================================
-- transcript analysis (sentiment+summary, as post-processing)
WITH transcr AS
    (SELECT TO_VARCHAR(AI_TRANSCRIBE(TO_FILE('@stage9', 'audio/audio-en.wav'))) AS tra)
SELECT
    AI_SENTIMENT(tra, ['Professionalism', 'Resolution', 'Funny']) AS sentiment,
    AI_COMPLETE('mistral-7b', CONCAT('Summarize how the agent can improve in 50 words', tra)) AS assessment
FROM transcr;