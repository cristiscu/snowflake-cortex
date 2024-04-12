import os
import streamlit as st
from openai import OpenAI

st.header("English-to-French Translator")

if prompt := st.text_area(" ", value="How are you doing?", height=300):
    prompt = f"Translate from English to French, with nothing else: {prompt}"

    # interact w/ Completion API on ChatGPT/OpenAI API
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    r = client.chat.completions.create(
        model="gpt-4-turbo-2024-04-09",
        messages=[{"role": "user", "content": prompt}])
    response = r.choices[0].message.content
    st.write(response)
