import streamlit as st
import pymysql
import pyodbc
from sql_rag import MariaDB
import io
from stqdm import stqdm
from sql_rag import  Database , MariaDB
from compare_llm import compare_llm_results 
import pandas as pd
import uuid
import requests
import json
# from auth import get_token
import os


session_id = st.session_state.session_id if 'session_id' in st.session_state else None
if session_id is None:
    session_id = str(uuid.uuid4())
    st.session_state.session_id = session_id

def get_csv_from_front_directory(file_name):
    front_directory = "Reports"
    file_name = f'output{file_name}.csv'
    file_path = os.path.join(front_directory, file_name)
    
    # Ensure the directory exists, if not, create it
    if not os.path.exists(front_directory):
        os.makedirs(front_directory)
    try:
        if file_name.endswith('.csv'):
            with open(file_path, 'rb') as file:
                return file.read()  # Return the binary data of the CSV file
        else:
            return None
    except FileNotFoundError:
        return None

def read_queries(file):
    try:
        questions = []
        queries = []
        lines = file.getvalue().decode("utf-8").split('\n')

        question = None
        query = None

        for line in lines:
            line = line.strip()
            if line.startswith("question"):
                if question is not None and query is not None:
                    questions.append(question)
                    queries.append(query)
                question = line.split(":")[1].strip()
            elif line.startswith("query"):
                query = line.split(":")[1].strip()
            elif line:
                    raise ValueError("Invalid line format")

        # Add the last question and query
        if question is not None and query is not None:
            questions.append(question)
            queries.append(query)
        st.success("Queries parsed successfully!")
    except:
        st.error("Error: Failed to parse queries from the input file ")
        st.info("Please ensure the structure of the input file matches the following format:\n\n"
                        "question1: <Your question here>\n\n"
                        "query1: <Your query here>\n\n"
                        "Example:\n"
                        "question1: Count the total number of apartment bookings.\n\n"
                        "query1: SELECT COUNT(*) AS total_bookings FROM bookings;")

    return questions, queries

def render_results(llm_response , result , df ,  i , count):
    st.write(f"Question {count}: {i}")

    st.header("Mysql Query")
    st.write(llm_response)
    # df1 = pd.read_json(df)
    st.header("Database Results")
    # print("-----------")
    # print(df)
    st.table(df)
    
    st.header("Explanation")
    st.write(result)

def download_report(session_id):
    # dir = "Front"
    # get_report(session_id)
    btn = st.download_button(
        label="Download Report",
        data=get_csv_from_front_directory(session_id),
        file_name="output.csv",
        mime="text/csv"
    )
    st.session_state.session_id = str(uuid.uuid4())

def extract_questions_queries(data):
    try:
        lines = data.split('\n')
        questions = []
        queries = []

        current_question = None
        current_query = None

        for line in lines:
            line = line.strip()
            if line.startswith("question"):
                if current_question is not None and current_query is not None:
                    questions.append(current_question)
                    queries.append(current_query)
                current_question = line.split(":")[1].strip()
            elif line.startswith("query"):
                current_query = line.split(":")[1].strip()
            elif line:
                raise ValueError("Invalid line format")

        if current_question is not None and current_query is not None:
            questions.append(current_question)
            queries.append(current_query)
        st.success("Queries parsed successfully!")

    except:
        st.error("Error: Failed to parse queries from the input filed ")
        st.info("Please ensure the structure of the input file matches the following format:\n\n"
                        "question1: <Your question here>\n\n"
                        "query1: <Your query here>\n\n"
                        "Example:\n"
                        "question1: Count the total number of apartment bookings.\n\n"
                        "query1: SELECT COUNT(*) AS total_bookings FROM bookings;")

    return questions, queries

def pass_data(db , questions , queries , session_id , db_schema):
    count = 0
    for i in stqdm(range(len(questions))):
        count += 1
        response , explanations , df = compare_llm_results(db, queries[i], questions[i], db_schema, session_id)
        # print(response , explanations , df)
        render_results(response, explanations, df, questions[i], count)
    download_report(session_id)




def maria_page():
    
    # Initialize session state
    if 'maria_connection' not in st.session_state:
        st.session_state.maria_connection = False
    

    st.sidebar.header("MariaDB Configuration")
    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.text_input("Port", value="3306")
    user = st.sidebar.text_input("Username", value="root")
    password = st.sidebar.text_input("Password", type="password")
    database = st.sidebar.text_input("Database")

    db_schema = st.text_area("MySQL Database Schema")
    txt_file = st.file_uploader("Upload Query File (.txt)", type="txt")
    single = st.text_area("Questions and Queries")
    button = st.button("Submit")

    if st.sidebar.button("Connect to MariaDB"):
        if not host or not user or not database or not password:
            st.sidebar.warning("Please fill all the fields.")
        else:
            try:
                conn = MariaDB(host , user , password , database)
                if conn.is_connected():
                    st.session_state.maria_connection = conn
                    
                    st.sidebar.success("Connected.")
                else:
                    st.sidebar.success("Not Connected.")
            except Exception as e:
                st.sidebar.success(f"Not Connected. {e}")
    if button:
        if st.session_state.maria_connection :
            if txt_file or single:
                try:
                    if single :
                        
                        questions, queries = extract_questions_queries(single)
                    else:
                        
                        questions, queries = read_queries(txt_file)
                    pass_data(st.session_state.maria_connection , questions ,queries , session_id , db_schema)
                except Exception as e:
                    st.error(f"Something Went Wrong : {e}")
            else:
                st.warning("Please provide input in any form")
        else:
            st.warning("Please connect DB first.")

        
        

def ms_sql_page():
    st.sidebar.header("MS SQL Configuration")
    server = st.sidebar.text_input("Server")
    database = st.sidebar.text_input("Database")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Connect to MS SQL Server"):
        
        try:
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            cursor = conn.cursor()
            st.write("Connected to MS SQL Server!")
        except:
            st.write("Connected to MS SQL Server!")

        st.header("MS SQL Query")
        query = st.text_area("Enter your SQL query here:")
        if st.button("Submit"):
            cursor.execute(query)
            result = cursor.fetchall()
            st.write(result)

def db_creation_page():
    st.header("Database Creation Page")
    # Add database creation functionality here

# Main menu
menu = ["MariaDB", "MS SQL", "DB Creation"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "MariaDB":
    maria_page()
elif choice == "MS SQL":
    ms_sql_page()
elif choice == "DB Creation":
    db_creation_page()
