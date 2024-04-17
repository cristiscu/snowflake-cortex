# ChatGPT Account Configuration

These steps are optional, if you need to experiment with the OpenAI API in your Python code.

(1) Login with your Microsoft or Google account at [**platform.openai.com**](https://platform.openai.com/). For new accounts, OpenAI may still offer US $5 credits for max 3 months. Otherwise provision your account with $5 from a credit card (it should be more than enough for all kind of experiments).

(2) Create a new secret key in the *API keys* screen. Copy and save that key into a **OPENAI_API_KEY** environment variable only you may have access to:

![ChatGPT API Key](./.images/credentials2.png)

Remark that once you create the secret online, you can no longer see it in clear, this was the only time!

(3) If you use Streamlit with a TOML file, you may need to create a ".streamlit" subfolder with a **.streamlit/secrets.toml** text file and add the following setting:

**`OPENAI_API_KEY = "sk-Z5I..."`**

