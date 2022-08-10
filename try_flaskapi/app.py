from flask import Flask
import psycopg2
import json
import sys
from airflow.models import Variable
from markupsafe import escape

app = Flask(__name__)

psql_host = Variable.get("psql_host")
psql_port = int(Variable.get("psql_port"))
psql_user = Variable.get("psql_user")
psql_password = Variable.get("psql_password")
psql_db = Variable.get("psql_db")

@app.route("/orders/<order_date>")
def orders(order_date):
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