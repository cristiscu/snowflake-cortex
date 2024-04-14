# Questions (online and here):
# ============================
# How are data and metadata-based applications deployed in the "Streamlit for Snowflake" Udemy course from January 2024?
# What about the hierarchical data viewer in the same course?
# When was the French revolution?

import asyncio
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import os
import streamlit as st
from llama_index.llms.openai import OpenAI
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core import ServiceContext, SimpleDirectoryReader, TreeIndex

def getQueryEngine():
    # connect to ChatGPT
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    # create and save index (as knowledge base)
    reader = SimpleDirectoryReader("./spool")
    documents = reader.load_data()
    context = ServiceContext.from_defaults(llm=client)
    index = TreeIndex.from_documents(documents, service_context=context)
    index.storage_context.persist(persist_dir="./kb")

    # load index (as query engine)
    context = StorageContext.from_defaults(persist_dir="./kb")
    index = load_index_from_storage(context)
    return index.as_query_engine()


st.title("ChatGPT Agent for Custom Content")

if "query_engine" not in st.session_state:
    st.session_state.query_engine = getQueryEngine()
    
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system",
        "content": ("Your purpose is to answer questions about specific documents only. "
        "Please answer the user's questions based on what you know about the document. "
        "If the question is outside the scope of the document, please politely decline. "
        "If you don't know the answer, say `I don't know`.")}]

for message in st.session_state.messages:
    if message["role"] in ["user", "assistant"]:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        response = st.session_state.query_engine.query(prompt)
        st.empty().markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
