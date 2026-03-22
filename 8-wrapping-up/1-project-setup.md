# Project Setup Instructions

[![Watch Here](../.images/youtube-1.png)](https://www.youtube.com/watch?v=FknW7jc0M24&list=PLWwulQvNh3MUNVsS3MoRcTecByVnX_fm5&index=2&t=5s&pp=gAQBiAQB)

(1) Install and use a [**Python version 3.11**](https://www.python.org/downloads/release/python-3110/).  

The latest versions are not usually installed right away in Snowflake, or used for the latest package dependencies.  

(2) Install and configure [**Visual Studio Code (VSCode)**](https://code.visualstudio.com/), which is a free source-code editor we will use, mostly for SQL and Python code snippets.  

(3) From a *Terminal* window in VSCode, switch to the folder where you keep most of your projects (mine is "C:\Projects"), and clone the current public *GitHub repository*:  

**`git clone https://github.com/cristiscu/snowflake-cortex.git`**

Then use *File > Open Folder...* to access the newly created *snowflake-cortex/* directory.  

(4) Open another *Terminal* window in VSCode, and check the Python version (make sure is the one you installed):

**`python --version`**

Also update **pip**:

**`python -m pip install --upgrade pip`**

(5) Create a **virtual environment** for this Python version, from the root folder. Virtual environments will keep only specific Python libraries for a project.  

**`python -m venv venv`**  

A new *venv/* folder was created (and already added to *.gitignore*!). Select the new virtual environment (your prompt should get a *"(venv)"* prefix) with:  

**`venv/scripts/activate`**  

You may later deactivate it with:  

**`deactivate venv`**  

(6) Install all Python dependencies for this project (this may take a while) with:  

**`pip install -r requirements.txt`**

Show all installed versions:

**`python list`**  

Show details for one single installed package (Snowpark):

**`python show snowflake-snowpark-python`**  
