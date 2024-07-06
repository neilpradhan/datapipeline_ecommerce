from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, PrimaryKeyConstraint, text



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(datetime.today().year, datetime.today().month, datetime.today().day),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    description='A simple eCommerce data pipeline',
    schedule_interval=timedelta(minutes=1),
)

def validate_inventory():
    cleaned_data_dir = "/opt/airflow/processed_data/cleaned_data"
    bad_data_dir = "/opt/airflow/processed_data/bad_data"
    os.makedirs(cleaned_data_dir, exist_ok=True)
    os.makedirs(bad_data_dir, exist_ok=True)

    df = pd.read_csv('/opt/airflow/data/inventory.csv')
    df.rename(columns={'quantity': 'inventory_quantity'}, inplace=True)
    bad_indices = []
    comments = []

    def add_bad_row(index, comment):
        if index not in bad_indices:
            bad_indices.append(index)
            comments.append(comment)

    for idx, value in df['productId'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "productId is not a string")

    for idx, value in df['name'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "name is not a string")

    for idx, value in df['inventory_quantity'].items():
        if not isinstance(value, int) or value < 0:
            add_bad_row(idx, "quantity is not a non-negative integer")

    for idx, value in df['category'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "category is not a string")

    for idx, value in df['subCategory'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "subCategory is not a string")

    bad_rows = df.loc[bad_indices].copy()
    bad_rows['comment'] = comments

    cleaned_rows = df.drop(bad_indices)

    cleaned_rows.to_csv(os.path.join(cleaned_data_dir, 'cleaned_inventory.csv'), index=False)
    bad_rows.to_csv(os.path.join(bad_data_dir, 'bad_inventory.csv'), index=False)

def validate_orders():
    cleaned_data_dir = "/opt/airflow/processed_data/cleaned_data"
    bad_data_dir = "/opt/airflow/processed_data/bad_data"
    os.makedirs(cleaned_data_dir, exist_ok=True)
    os.makedirs(bad_data_dir, exist_ok=True)

    df = pd.read_csv('/opt/airflow/data/orders.csv')
    df.rename(columns={'quantity': 'order_quantity'}, inplace=True)
    bad_indices = []
    comments = []
    original_dateTimes = []

    def add_bad_row(index, comment, original_dateTime=None):
        if index not in bad_indices:
            bad_indices.append(index)
            comments.append(comment)
            original_dateTimes.append(original_dateTime)

    for idx, value in df['orderId'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "orderId is not a string")

    for idx, value in df['productId'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "productId is not a string")

    for idx, value in df['currency'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "currency is not a string")

    for idx, value in df['order_quantity'].items():
        if not isinstance(value, int) or value < 0:
            add_bad_row(idx, "quantity is not a positive integer")

    for idx, value in df['shippingCost'].items():
        if not isinstance(value, (int, float)):
            add_bad_row(idx, "shippingCost is not a float or int")

    for idx, value in df['amount'].items():
        if not isinstance(value, (int, float)) or value < 0:
            add_bad_row(idx, "amount is not a positive float or int")

    for idx, value in df['channel'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "channel is not a string")

    for idx, value in df['channelGroup'].items():
        if not isinstance(value, str):
            add_bad_row(idx, "channelGroup is not a string")

    for idx, value in df['campaign'].items():
        if not (isinstance(value, str) or pd.isna(value)):
            add_bad_row(idx, "campaign is not a string or NaN")

    original_dateTime_col = df['dateTime'].copy()
    df['dateTime'] = pd.to_datetime(df['dateTime'], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
    for idx, value in df['dateTime'].items():
        if pd.isna(value):
            add_bad_row(idx, "dateTime is not in the correct format", original_dateTime_col[idx])

    bad_rows = df.loc[bad_indices].copy()
    bad_rows['comment'] = comments
    bad_rows['original_dateTime'] = original_dateTimes

    cleaned_rows = df.drop(bad_indices)

    cleaned_rows.to_csv(os.path.join(cleaned_data_dir, 'cleaned_orders.csv'), index=False)
    bad_rows.to_csv(os.path.join(bad_data_dir, 'bad_orders.csv'), index=False)

def merge_data():
    cleaned_data_dir = "/opt/airflow/processed_data/cleaned_data"
    combined_data_dir = "/opt/airflow/processed_data/combined_data"
    os.makedirs(combined_data_dir, exist_ok=True)

    cleaned_orders = pd.read_csv(os.path.join(cleaned_data_dir, 'cleaned_orders.csv'))
    cleaned_inventory = pd.read_csv(os.path.join(cleaned_data_dir, 'cleaned_inventory.csv'))

    combined_data = pd.merge(cleaned_orders, cleaned_inventory, on='productId')
    combined_data.to_csv(os.path.join(combined_data_dir, 'combined_data.csv'), index=False)

def load_to_db():
    combined_data_dir = "/opt/airflow/processed_data/combined_data"
    combined_data = pd.read_csv(os.path.join(combined_data_dir, 'combined_data.csv'))

    engine = create_engine('sqlite:////opt/airflow/airflow.db')
    metadata = MetaData()

    combined_table = Table('combined_data', metadata,
                           Column('orderId', String),
                           Column('productId', String),
                           Column('currency', String),
                           Column('order_quantity', Integer),
                           Column('shippingCost', Float),
                           Column('amount', Float),
                           Column('channel', String),
                           Column('channelGroup', String),
                           Column('campaign', String),
                           Column('dateTime', String),
                           Column('name', String),
                           Column('inventory_quantity', Integer),
                           Column('category', String),
                           Column('subCategory', String),
                           PrimaryKeyConstraint('orderId', 'productId', 'dateTime'))

    metadata.create_all(engine)
    combined_data.to_sql('combined_data', con=engine, if_exists='replace', index=False)







def generate_reports():
    engine = create_engine('sqlite:////opt/airflow/airflow.db')
    connection = engine.connect()

    queries = {
        "Total Order Quantity by Product": """
            SELECT productId, SUM(order_quantity) AS total_quantity
            FROM combined_data
            GROUP BY productId;
        """,
        "Total Sales Amount by Category": """
            SELECT category, SUM(amount) AS total_sales
            FROM combined_data
            GROUP BY category;
        """,
        "Top 10 Products by Sales Amount": """
            SELECT productId, SUM(amount) AS total_sales
            FROM combined_data
            GROUP BY productId
            ORDER BY total_sales DESC
            LIMIT 10;
        """
    }

    results_dir = "/opt/airflow/processed_data/reports"
    os.makedirs(results_dir, exist_ok=True)

    for title, query in queries.items():
        result = connection.execute(query).fetchall()
        with open(os.path.join(results_dir, f"{title.replace(' ', '_').lower()}.txt"), 'w') as f:
            for row in result:
                f.write(f"{row}\n")
    connection.close()





validate_inventory_task = PythonOperator(
    task_id='validate_inventory',
    python_callable=validate_inventory,
    dag=dag,
)

validate_orders_task = PythonOperator(
    task_id='validate_orders',
    python_callable=validate_orders,
    dag=dag,
)

merge_data_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
)

load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    dag=dag,
)


generate_reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

validate_inventory_task >> validate_orders_task >> merge_data_task >> load_to_db_task >> generate_reports_task

