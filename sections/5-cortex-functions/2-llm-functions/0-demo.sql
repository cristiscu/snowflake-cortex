USE SCHEMA SNOWFLAKE.CORTEX;

SET review = $$
I've been a customer for less than a year and I have never had to visit a branch this much in my lifetime. I've had my banking card locked THREE times for fraud. I'm canceling both my debit and credit cards ASAP when I can access a branch.
$$;

SELECT COMPLETE('mistral-large', 'Is this bank customer cancelling his service? ' || $review) as completion;

SELECT EXTRACT_ANSWER($review, 'Why is this customer not paying his bills?') as answer;

SELECT SENTIMENT($review) as mood;

SELECT SUMMARIZE($review) as summary;

SELECT TRANSLATE($review, 'en', 'fr') as translation;
SELECT SNOWFLAKE.CORTEX.TRANSLATE($review, 'en', 'fr') as translation;
