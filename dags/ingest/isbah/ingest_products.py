import psycopg2
import csv
import sys
from airflow.models import Variable

psql_host = Variable.get("psql_host")
psql_port = int(Variable.get("psql_port"))
psql_user = Variable.get("psql_user")
psql_password = Variable.get("psql_password")
psql_db = Variable.get("psql_db")

conn = None
sql = """select
    d.order_id,
    a.product_id, 
    a.product_name, 
    a.supplier_id, 
    b.company_name , 
    a.category_id, 
    c.category_name, 
    a.quantity_per_unit, 
    a.unit_price, 
    a.units_in_stock, 
    a.units_on_order, 
    a.reorder_level, 
    a.discontinued 
    from products a 
    left join suppliers b on a.supplier_id = b.supplier_id 
    left join categories c on c.category_id = a.category_id
    left join order_details d on d.product_id = a.product_id
    left join orders e on e.order_id = d.order_id
    where cast(e.order_date as date)  = '"""+sys.argv[1]+"""'"""
csv_file_path = '/root/output/isbah/products/products_'+sys.argv[1]+'.csv'

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
    # New empty list called 'result'. This will be written to a file.
    result = list()

    # The row name is the first entry for each entity in the description tuple.
    column_names = list()
    for i in cursor.description:
        column_names.append(i[0])

    result.append(column_names)
    for row in rows:
        result.append(row)

    # Write result to file.
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in result:
            csvwriter.writerow(row)
else:
    print("No rows found for query: {}".format(sql))