import requests
import json
import mysql.connector
import re 

url = "http://localhost:11434/api/generate"

conn = mysql.connector.connect(
    host = "localhost" ,
    user = "root" , 
    password ="123Kshitiz@" ,
    database = "org"    
)

cursor  = conn.cursor()

def get_mysql_schema(cursor): 
    schema_info = ""
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        cursor.execute(f"DESCRIBE {table}")
        columns = cursor.fetchall()
        col_defs = ", ".join(f"{col[0]} {col[1]}" for col in columns)
        schema_info += f"\nTable: {table} ({col_defs})"
    
    return schema_info.strip()

schema = get_mysql_schema(cursor)


user_input = input("\n Ask your question: ")

prompt_input = f"""
You are a SQL expert. Based on the following MySQL database schema:

{schema}

Convert the following natual language into a accurate and MySQL query:

\"\"\"{user_input}\"\"\"
"""

new_data = {
    "model" :"llama3.2" , 
    "prompt" : prompt_input
}

response = requests.post(
    url , json = new_data , stream= True
)

complete_response = ""


if response.status_code == 200:
    print("Generated Text:", end=" ", flush=True)
    for line in response.iter_lines():
        if line:
            decoded_line = line.decode("utf-8")
            result = json.loads(decoded_line)
            generated_text = result.get("response", "")
            complete_response+=generated_text
            print(generated_text, end="", flush=True)
else:
    print("Error:", response.status_code, response.text)


matched_query = re.search(r"```sql\s*(.*?)\s*```", complete_response, re.DOTALL | re.IGNORECASE)

if matched_query:
    sql_query = matched_query.group(1).strip()
    print("\n Extracted SQL Query:\n", sql_query)

    try:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        print("\n Output : ")
        for r in results :
            print(r)
    except Exception as e :
        print("Execution error : " , e )

else :
    print("Not find a valid sql query ")




