# Project Setup Instructions

[![Watch Here](../.images/youtube-1.png)](https://www.youtube.com/watch?v=FknW7jc0M24&list=PLWwulQvNh3MUNVsS3MoRcTecByVnX_fm5&index=2&t=5s&pp=gAQBiAQB)

(1) Install and configure [**Visual Studio Code (VSCode)**](https://code.visualstudio.com/), which is a free source-code editor we will use, mostly for SQL and Python code snippets.  

(2) From a *Terminal* window in VSCode, switch to the folder where you keep most of your projects (mine is "C:\Projects"), and clone the current public *GitHub repository*:  

**`git clone https://github.com/cristiscu/snowflake-cortex.git`**

Then use *File > Open Folder...* to access the newly created *snowflake-cortex/* directory.  

(3) Install and use a [**Python version 3.9**](https://www.python.org/downloads/release/python-390/) for our project (as latest versions are not usually installed in Snowflake).  

(4) In a Terminal window from VSCode, create a **virtual environment** for this Python version, from the root folder. Virtual environments will keep only specific Python libraries for a project.  

If you have multiple installations of Python on your computer, a path to python.exe may be required. My 3.9 version was installed in the *~\AppData\Local\Programs\Python\Python39\\* folder, but yours could be elsewhere:  

**`~\AppData\Local\Programs\Python\Python39\python.exe -m venv venv`**  

A new *venv/* folder was created (and already added to *.gitignore*!). Select the new virtual environment (your prompt should get a *"(venv)"* prefix) with:  

**`venv/scripts/activate`**  

(5) Install all Python dependencies for this project (this may take a while) with:  

**`pip install -r requirements.txt`**

You may also want to update pip first:

**`python -m pip install --upgrade pip`**

Show all installed versions:

**`python list`**  

Show details for one single installed package (Snowpark):

**`python show snowflake-snowpark-python`**  
