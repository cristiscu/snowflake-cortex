select snowflake.cortex.summarize($$
I am a new customer for less than a year and I have never had to visit a branch this much in my lifetime. I've had my banking card locked THREE times for fraud. No fraudulent payments were made, nor anything else beside trying to put my card in my wallet on my phone. Since I live over an hour away from the nearest branch, they gave me a virtual appointment to unlock my card, and never gave my the joining code so I couldn't access it. I can't make any payments on my credit card because I can't access my online banking, so now they're adding interest to it. And as this is the third time my account was frozen for fraud, it has also been going on for TWO MONTHS. I'm canceling both my debit and credit cards ASAP when I can access a branch which I'll either have to miss work for as a contract worker, or go to my nearest city, carless, which is again over an hour away, 2 hours by bus, on the weekend.
$$) as summary;

select review, snowflake.cortex.summarize(review) as summary
from imdb.public.train_dataset
limit 3;
