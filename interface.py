import streamlit as st
import io
from stqdm import stqdm
from sql_rag import  Database 
from compare_llm import compare_llm_results 
import pandas as pd
import uuid
import requests
import json
# from auth import get_token
import os

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

# def process_request( db, queries, questions, db_schema , session_id):
#     url = "http://127.0.0.1:5000/process_request/"
#     data = {
#         "session_id": session_id,
#         "db": db,
#         "gpt_query": queries,
#         "question": questions,
#         "db_schema": db_schema
#     }
#     response  = requests.post(url, json=data)
#     print(response)
#     return response.json()


# def get_report(session_id: str):
#     endpoint_url = "http://localhost:5000/get_report/"  # Update with your actual endpoint URL
#     response = requests.get(endpoint_url + session_id)
#     if response.status_code == 200:
#         file_path = os.path.join('Front', f"output{session_id}.csv")
#         with open(file_path, "wb") as f:
#             f.write(response.content)
#         print("CSV file received and saved successfully")
#     else:
#         print("Failed to receive CSV file")
#     return file_path

    # print(f"Report saved to Front/output{session_id}.csv")

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

def pass_data(db , questions , queries , session_id):
    count = 0
    for i in stqdm(range(len(questions))):
        count += 1
        response , explanations , df = compare_llm_results(db, queries[i], questions[i], db_schema, session_id)
        # print(response , explanations , df)
        render_results(response, explanations, df, questions[i], count)
    download_report(session_id)


session_id = st.session_state.session_id if 'session_id' in st.session_state else None
if session_id is None:
    session_id = str(uuid.uuid4())
    st.session_state.session_id = session_id
print("Session ID: ",session_id)

st.title("Kamal LLM")
st.sidebar.header("MySQL Database Configuration")
db_host = st.sidebar.text_input("Database Host")
db_name = st.sidebar.text_input("Database Name")
db_user = st.sidebar.text_input("Database User")
db_password = st.sidebar.text_input("Database Password", type="password")


db_schema = st.text_area("MySQL Database Schema")
txt_file = st.file_uploader("Upload Query File (.txt)", type="txt")
single = st.text_area("Questions and Queries")

if st.button("Submit"):
    if not db_host or not db_user or not db_name or not db_password:
        st.sidebar.warning("Please fill all the fields.")
    else:
        if db_host and db_name and db_user and db_password:
            db = {
            "db_host": db_host,
            "db_name": db_name,
            "db_user": db_user,
            "db_password": db_password
            }
            try:    
                conn =  Database(host=db.get("db_host"), 
                                db_user=db.get("db_user"), 
                                db_name=db.get("db_name"), 
                                db_password=db.get("db_password"))
                st.sidebar.success("DB Connected Successfully.")

                
            except:
                st.sidebar.warning("DB Connection Failed.")
        if txt_file or single:
            try:
                if single :
                        questions, queries = extract_questions_queries(single)
                else:
                        questions, queries = read_queries(txt_file)
                pass_data(conn , questions ,queries , session_id)
                
            except Exception as e:
                st.error(f"Something Went Wrong : {e}")
                
        else:
            st.warning("Please provide input in any form")
      


# db_schema ="""
# CREATE TABLE Apartment_Buildings (
#     building_id INTEGER NOT NULL,
#     building_short_name CHAR(15),
#     building_full_name VARCHAR(80),
#     building_description VARCHAR(255),
#     building_address VARCHAR(255),
#     building_manager VARCHAR(50),
#     building_phone VARCHAR(80),
#     PRIMARY KEY (building_id),
#     UNIQUE (building_id)
# );

# CREATE TABLE Apartments (
#     apt_id INTEGER NOT NULL,
#     building_id INTEGER NOT NULL,
#     apt_type_code CHAR(15),
#     apt_number CHAR(10),
#     bathroom_count INTEGER,
#     bedroom_count INTEGER,
#     room_count CHAR(5),
#     PRIMARY KEY (apt_id),
#     UNIQUE (apt_id),
#     FOREIGN KEY (building_id) REFERENCES Apartment_Buildings (building_id)
# );

# CREATE TABLE Apartment_Facilities (
#     apt_id INTEGER NOT NULL,
#     facility_code CHAR(15) NOT NULL,
#     PRIMARY KEY (apt_id, facility_code),
#     FOREIGN KEY (apt_id) REFERENCES Apartments (apt_id)
# );

# CREATE TABLE Guests (
#     guest_id INTEGER NOT NULL,
#     gender_code CHAR(1),
#     guest_first_name VARCHAR(80),
#     guest_last_name VARCHAR(80),
#     date_of_birth DATETIME,
#     PRIMARY KEY (guest_id),
#     UNIQUE (guest_id)
# );

# CREATE TABLE Apartment_Bookings (
#     apt_booking_id INTEGER NOT NULL,
#     apt_id INTEGER,
#     guest_id INTEGER NOT NULL,
#     booking_status_code CHAR(15) NOT NULL,
#     booking_start_date DATETIME,
#     booking_end_date DATETIME,
#     PRIMARY KEY (apt_booking_id),
#     UNIQUE (apt_booking_id),
#     FOREIGN KEY (apt_id) REFERENCES Apartments (apt_id),
#     FOREIGN KEY (guest_id) REFERENCES Guests (guest_id)
# );

# CREATE TABLE View_Unit_Status (
#     apt_id INTEGER,
#     apt_booking_id INTEGER,
#     status_date DATETIME NOT NULL,
#     available_yn BOOLEAN,
#     PRIMARY KEY (status_date),
#     FOREIGN KEY (apt_id) REFERENCES Apartments (apt_id),
#     FOREIGN KEY (apt_booking_id) REFERENCES Apartment_Bookings (apt_booking_id)
# );


# """
            
# db = {
#         "db_host": "localhost",
#         "db_name": "apartments_data",
#         "db_user": "root",
#         "db_password": "admin"
#         }
# questions = ["total number of guests"]
# queries = ["select count(*) from guests;"]
# conn = Database(db.get("db_host"), 
#                         db_user=db.get("db_user"), 
#                         db_name=db.get("db_name"), 
#                         db_password=db.get("db_password"))
# print(compare_llm_results(conn , queries , questions , db_schema,session_id="001" ))