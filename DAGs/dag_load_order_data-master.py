from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
import random
from sqlalchemy import create_engine
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow_BI',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    }
schema = 'fine_delivery'

config = {
    "host"      : "database-1.cctetsurxox7.eu-north-1.rds.amazonaws.com",
    "database"  : "postgres",
    "user"      : "postgres",
    "password"  : "7454556qQ!",
    "port"      : "5432"
}

# def read_sql(sql, error=0, conf = config):
#     conn = psycopg2.connect(**conf)
#     try:
#         df = pd.read_sql(sql,conn)
#         conn.close()
#         return df   
#     except Exception as t:
#         print(t)
#         if error<100:
#             error+=1
#             conn.close()
#             time.sleep(1.5)
#             return read_sql(sql = sql,error = error,conf = conf)
#         else:
#             conn.close()


df_assortment = pd.read_csv('https://raw.githubusercontent.com/AntonMiniazev/Online_retail_Pipeline/main/project_notebooks/assortment_generator.csv', dtype=object, delimiter = ';', thousands=',')
df_delivery_types = pd.read_csv('https://raw.githubusercontent.com/AntonMiniazev/Online_retail_Pipeline/main/initial_data/delivery_types.csv', dtype=object, delimiter = ';', thousands=',')


def assign_values_within_day(df):
    num_orders = len(df)
    num_ones = int(num_orders * random.uniform(0.6, 0.85))  # Calculate number of 1s

    # Assign 1s and 2s randomly within the day
    values = random.choices([1, 2], k=num_orders)
    values[:num_ones] = [1] * num_ones  # Set the first num_ones elements to 1
    return values

def get_random_positions(delivery_type, assortment = df_assortment):
    if delivery_type == 1:
        num_ids = random.randint(2, 12)  # Bikes will have positions between 2 and 12
    else:
        num_ids = random.randint(5, 20)  # Cars will have positions between 5 and 20
    random_positions = df_assortment['product_id'].sample(n=num_ids).tolist()
    return random_positions

def add_values_to_column(df, values):
    num_rows = len(values)
    num_cols = len(df.columns)
    repeated_df = pd.concat([df] * num_rows, ignore_index=True)
    repeated_df['product_id'] = values[:num_rows]
    return repeated_df

def add_positions(df):
    df_with_positions = pd.DataFrame(columns=df.columns)
    for id in df['order_id']:
        tmp_df = df.query('order_id == @id')
        tmp_df = add_values_to_column(tmp_df,get_random_positions(tmp_df.iloc[0]['delivery_type']))
        df_with_positions = pd.concat([df_with_positions,tmp_df], ignore_index=True)
    return df_with_positions

def add_quantity(row):
    prod_id = row['product_id']
    product_limits = df_assortment.query('product_id == @prod_id')
    if row['delivery_type'] == 1:
        num_q = random.randint(int(product_limits.iloc[0]['limit_min']), int(product_limits.iloc[0]['limit_bike_max']))
    else:
        num_q = random.randint(int(product_limits.iloc[0]['limit_min']), int(product_limits.iloc[0]['limit_car_max']))
    return num_q

def orders_dates(start_date, end_date, df_assortment, first_order = 10000):
    # Convert start_date and end_date strings to datetime objects
    start_date = datetime.strptime(start_date, "%d.%m.%Y")
    end_date = datetime.strptime(end_date, "%d.%m.%Y")

    # Calculate the number of days between start_date and end_date
    num_days = (end_date - start_date).days + 1

    gen_orders = []

    # Generate rows for each date in the range
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        num_orders = random.randint(20, 35)  # Random number of orders between 20 and 35
        orders = [(first_order + j, current_date) for j in range(num_orders)]
        gen_orders.extend(orders)
        first_order += num_orders
    
    df = pd.DataFrame(data=gen_orders, columns=['order_id','delivery_date'])
    
    num_orders = len(df)
    zone_ids = [random.randint(1, 5) for _ in range(num_orders)]
    df['zone_id'] = zone_ids
    
    values = []
    for date in df['delivery_date'].unique():
        tmp_df = df.query('delivery_date == @date')
        x = assign_values_within_day(tmp_df)
        values.extend(x)
    df['delivery_type'] = values

    df_with_positions = add_positions(df)
    df_with_positions['quantity'] = df_with_positions.apply(add_quantity,axis=1)
    
    df_with_positions = df_with_positions.merge(df_assortment[['product_id','selling_price','cost_of_sales']], how='left',on='product_id')
    
    df_with_positions['total_price'] = df_with_positions['quantity'] * df_with_positions['selling_price'].astype('float16')
    df_with_positions['total_cost'] = df_with_positions['quantity'] * df_with_positions['cost_of_sales'].astype('float16')
    
    df_with_positions['delivery_date'] = pd.to_datetime(df_with_positions['delivery_date'],format="%d.%m.%Y").dt.date    
    
    return df_with_positions   

def add_churn_flag(clients):
    
    upd_clients = clients.copy()
    
    # Identify clients who already have 'churn_flag' = True
    existing_churned = clients[clients['churn_flag'] == True]['client_id']

    # Identify the remaining clients
    remaining_clients = clients[~clients['client_id'].isin(existing_churned)]['client_id']

    # Calculate the number of new clients to assign True for churn_flag
    num_clients = len(remaining_clients)
    min_true_count = int(0.01 * num_clients)
    max_true_count = int(0.03 * num_clients)
    true_count = np.random.randint(min_true_count, max_true_count + 1)

    # Randomly choose clients from the remaining_clients list to assign True
    new_churned_clients = np.random.choice(remaining_clients, size=true_count, replace=False)

    # Update the clients in new_churned_clients to True
    upd_clients.loc[clients['client_id'].isin(new_churned_clients), 'churn_flag'] = 'True'
    
    return upd_clients

def assign_clients_to_orders(orders, clients):
    # Determine the number of new clients (15% of orders)
    num_new_clients = int(0.15 * len(orders))
    
    # Determine the starting client_id for new clients
    if not clients.empty:
        starting_client_id = clients['client_id'].max() + 1
    else:
        starting_client_id = 10000  # assuming a default starting point if no existing clients
    
    # Generate new client_ids for the new clients
    new_clients = list(range(starting_client_id, starting_client_id + num_new_clients))
    
    # For the remaining 85% of orders, randomly assign from existing clients (excluding churned clients)
    existing_clients = clients.loc[clients.get('churn_flag') != True, 'client_id'].tolist()
    
    # If there are no existing clients, all new orders will get new client IDs
    if not existing_clients:
        order_clients = new_clients
    else:
        remaining_orders = len(orders) - num_new_clients
        random_existing_clients = random.choices(existing_clients, k=remaining_orders)
        order_clients = new_clients + random_existing_clients
    
    # Assign the client_ids to the orders
    orders['client_id'] = order_clients
    
    # Create a DataFrame for the new clients
    new_clients_df = pd.DataFrame({'client_id': new_clients, 'churn_flag': ['False']*len(new_clients)})
    
    # Update the original clients DataFrame
    clients = pd.concat([clients, new_clients_df]).reset_index(drop=True)
    
    return orders, clients

def upsert_clients_bulk(dataframe, table_name,schema):
    # Using Airflow's PostgresHook to get the connection
    hook = PostgresHook(postgres_conn_id="fine_delivery_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # SQL template for the upsert operation
    sql_template = f"""
        INSERT INTO {schema}.{table_name} (client_id, churn_flag)
        VALUES %s
        ON CONFLICT (client_id) 
        DO UPDATE SET churn_flag = EXCLUDED.churn_flag;
    """
    
    # Convert dataframe to list of tuples
    records = list(dataframe.itertuples(index=False, name=None))
    
    # Use execute_values for batch insert and update
    execute_values(cursor, sql_template, records)
    conn.commit()

    cursor.close()
    conn.close()

def insert_new_orders(df):
    # Using Airflow's PostgresHook to get the connection
    #hook = PostgresHook(postgres_conn_id="fine_delivery_connection")
    engine = create_engine('postgresql+psycopg2://', connect_args={
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '7454556qQ!',
        'host': 'database-1.cctetsurxox7.eu-north-1.rds.amazonaws.com',
        'port': '5432'})

    # Write the DataFrame to a temporary table in the database
    df.to_sql('temp_orders', engine, schema=schema, if_exists='replace', index=False)

    # Use INSERT INTO ... ON CONFLICT to insert data from the temporary table 
    # to the main table, while avoiding insertion of duplicate order_id
    insert_sql = f"""
        INSERT INTO {schema}.orders (order_id, delivery_date, delivery_type, zone_id, total_value, client_id)
        SELECT *
        FROM {schema}.temp_orders
        ON CONFLICT (order_id) 
        DO NOTHING;
    """
    
    with engine.connect() as connection:
        connection.execute(insert_sql)
        
        # Optionally, you can drop the temporary table after the operation
        connection.execute(f"DROP TABLE {schema}.temp_orders;")
    engine.dispose()

def insert_new_products(df):
    
    # Using Airflow's PostgresHook to get the SQLAlchemy engine
    #hook = PostgresHook(postgres_conn_id="fine_delivery_connection")
    engine = create_engine('postgresql+psycopg2://', connect_args={
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '7454556qQ!',
        'host': 'database-1.cctetsurxox7.eu-north-1.rds.amazonaws.com',
        'port': '5432'})

    # Write the DataFrame to a temporary table in the database
    df.to_sql('temp_products', engine, schema=schema, if_exists='replace', index=False)

    # Use INSERT INTO ... ON CONFLICT to insert data from the temporary table 
    # to the main table. If you want to avoid insertion of duplicate product_id, 
    # make sure to include the ON CONFLICT clause. The current code lacks that.
    insert_sql = f"""
        INSERT INTO {schema}.products (product_id, order_id, quantity, selling_price, cost_of_sales, total_price, total_cost)
        SELECT *
        FROM {schema}.temp_products;
    """
    
    with engine.connect() as connection:
        connection.execute(insert_sql)
        
        # Optionally, you can drop the temporary table after the operation
        connection.execute(f"DROP TABLE {schema}.temp_products;")
    engine.dispose()

@dag(dag_id='dag_load_order_data_prod', 
     default_args=default_args, 
     start_date=datetime(2023, 3, 27),
    # end_date=datetime(2023, 4, 5),
     schedule_interval='0 0 * * 1',
     max_active_runs=1
     )


def load_order_data():

    @task(multiple_outputs=True)
    def order_generation(logical_date=None, ti=None):
        lastweek_ds = logical_date
        ds = logical_date + timedelta(days=6)

        hook = PostgresHook(postgres_conn_id="fine_delivery_connection")
        conn = hook.get_conn()

        first_order_query = f"""
            SELECT
            MAX(order_id)+1
            FROM {schema}.orders"""
        
        initial_clients_query = f"""
            SELECT
            *
            FROM {schema}.clients"""

        first_order = pd.read_sql_query(first_order_query, conn).iloc[0, 0]
        initial_clients = pd.read_sql_query(initial_clients_query, conn)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> HERE:", lastweek_ds.strftime('%d.%m.%Y'), ds.strftime('%d.%m.%Y'))

        df_orders = orders_dates(lastweek_ds.strftime('%d.%m.%Y'), ds.strftime('%d.%m.%Y'), df_assortment, first_order)
        
        f_orders = df_orders.groupby(['order_id','delivery_date','delivery_type','zone_id']).agg({'total_price':'sum'}).reset_index().rename(columns={'total_price':'total_value'})
        f_products = df_orders[['product_id'
                                            ,'order_id'
                                            ,'quantity'
                                            ,'selling_price'
                                            ,'cost_of_sales'
                                            ,'total_price'
                                            ,'total_cost']]

        f_products['product_id'] = f_products['product_id'].astype(int)
        f_products['order_id'] = f_products['order_id'].astype(int)
        f_products['selling_price'] = f_products['selling_price'].astype(float)
        f_products['cost_of_sales'] = f_products['cost_of_sales'].astype(float)

        updated_clients = add_churn_flag(initial_clients)

        f_orders, f_clients = assign_clients_to_orders(f_orders, updated_clients)
        
        return {"order_table": f_orders, "products_table": f_products, "clients_table": f_clients}

    @task()
    def load_data(f_orders, f_products, f_clients, schema = schema):

        upsert_clients_bulk(f_clients,'clients',schema=schema)
        print(1, "is done")
        insert_new_orders(f_orders)
        print(2, "is done")
        insert_new_products(f_products)
        print(3, "is done")

    f_tables = order_generation()

    load_data(f_tables["order_table"],f_tables["products_table"],f_tables["clients_table"])

dag_load_order_data_prod = load_order_data()