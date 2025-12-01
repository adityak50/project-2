import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Project 2", layout="wide")


PASSWORD = "aditya26"

def check_password():
    """Returns True if the user entered the correct password."""
    pw_input = st.text_input("Enter password:", type="password")
    if pw_input == PASSWORD:
        return True
    elif pw_input != "":
        st.error("Incorrect password.")
    return False

if not check_password():
    st.stop()

st.success("Logged in successfully!")


connection_string = st.secrets["DATABASE_URL"]


engine = create_engine(connection_string)

left_col, right_col = st.columns(2)

with left_col:
    st.header("Run SQL Query")
    user_query = st.text_area("Enter SQL query:", "SELECT * FROM project2_table;")
    run_button = st.button("Run Query")

with right_col:
    st.header("Query Output")
    if run_button:
        try:
            df = pd.read_sql(user_query, engine)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error: {e}")

import openai

st.header("ChatGPT Assistant")

question = st.text_input("Ask ChatGPT a question:")

if st.button("Ask ChatGPT"):
    try:
        api_key = st.secrets["OPENAI_API_KEY"]
        client = openai.OpenAI(api_key=api_key)

        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": question}
            ]
        )

        st.write("### ChatGPT Response:")
        st.write(completion.choices[0].message["content"])

    except Exception as e:
        st.error(f"Error: {e}")
