select snowflake.cortex.sentiment('this is a great bank') as mood;

select snowflake.cortex.sentiment($$
I have been using this bank for over 35 years and only had a couple of issues that were taken care of swiftly and with the most upmost care and helpfulness. The thing I love about this bank is they work with you not against you. Prime example if you short a dollar most banks would charge a full fee where this bank would use a minus proving they would sooner have customers than grab money for a minor mistake like most banks.
$$) as mood;

select snowflake.cortex.sentiment($$
I am a new customer for less than a year and I have never had to visit a branch this much in my lifetime. I've had my banking card locked THREE times for fraud. No fraudulent payments were made, nor anything else beside trying to put my card in my wallet on my phone. Since I live over an hour away from the nearest branch, they gave me a virtual appointment to unlock my card, and never gave my the joining code so I couldn't access it. I can't make any payments on my credit card because I can't access my online banking, so now they're adding interest to it. And as this is the third time my account was frozen for fraud, it has also been going on for TWO MONTHS. I'm canceling both my debit and credit cards ASAP when I can access a branch which I'll either have to miss work for as a contract worker, or go to my nearest city, carless, which is again over an hour away, 2 hours by bus, on the weekend.
$$) as mood;

select review, sentiment,
    snowflake.cortex.sentiment(review) as mood
    -- openai_db.public.openai(review) as chatgpt_mood
from imdb.public.train_dataset
limit 5;
