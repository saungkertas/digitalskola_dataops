from fastapi import FastAPI
import uvicorn
import psycopg2
import json
import sys
from airflow.models import Variable
from markupsafe import escape

app = FastAPI()


psql_host = Variable.get("psql_host")
psql_port = int(Variable.get("psql_port"))
psql_user = Variable.get("psql_user")
psql_password = Variable.get("psql_password")
psql_db = Variable.get("psql_db")

@app.get("/")
def home():
    return {"message": "Hello this is efrad"}

@app.get("/orders/")
def orders(order_date:str):
    conn = None
    sql = f"""select order_id,customer_id,employee_id,cast(order_date as varchar) order_date,cast(required_date as varchar) required_date,cast(shipped_date as varchar) shipped_date,ship_country from orders o where cast(order_date as date) = '{escape(order_date)}'"""
    
    try:
        #connection to PostgreSQL
        conn = psycopg2.connect(
            user=psql_user,
            password=psql_password,
            dbname=psql_db,
            host=psql_host,
            port=psql_port
        )

        #run PostgreSQL query
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    finally:
        if conn:
            conn.close()

    # Continue only if there are rows returned.
    if rows:
       return(json.dumps(rows))
    else:
        return("No rows found for query: {}".format(sql))

if __name__ == '__main__':
       uvicorn.run(app=app, host="35.184.141.204", port=5000)
