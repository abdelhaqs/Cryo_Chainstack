from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import cryo
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL and PostgreSQL connection details from environment variables
eth_rpc = os.getenv("ETH_RPC")
LAST_PROCESSED_BLOCK_FILE = 'last_processed_block.txt'

def get_last_processed_block():
    if os.path.exists(LAST_PROCESSED_BLOCK_FILE):
        with open(LAST_PROCESSED_BLOCK_FILE, 'r') as file:
            return int(file.read().strip())
    else:
        return 0  # Starting block number for the initial run

def set_last_processed_block(block_number):
    with open(LAST_PROCESSED_BLOCK_FILE, 'w') as file:
        file.write(str(block_number))

def collect_blocks(**kwargs):
    start_block = kwargs['start_block']
    end_block = kwargs['end_block']
    blocks_data = cryo.freeze(
        "blocks",
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc,
        output_dir="raw/blocks",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

def collect_logs(**kwargs):
    start_block = kwargs['start_block']
    end_block = kwargs['end_block']
    logs_data = cryo.freeze(
        "logs", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/logs",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

def collect_transactions(**kwargs):
    start_block = kwargs['start_block']
    end_block = kwargs['end_block']
    transactions_data = cryo.freeze(
        "transactions", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/transactions",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

def create_or_replace_table_logs():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_logs AS
    SELECT *
    FROM read_parquet(
      'raw/logs/*.parquet'
      )
    '''
    with duckdb.connect('data/BLOCKCHAIN.db') as con:
        con.sql(sql_query)

def create_or_replace_table_blocks():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_blocks AS
    SELECT *
    FROM read_parquet(
      'raw/blocks/*.parquet'
      )
    '''
    with duckdb.connect('data/BLOCKCHAIN.db') as con:
        con.sql(sql_query)

def create_or_replace_table_transactions():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_transactions AS
    SELECT *
    FROM read_parquet(
      'raw/transactions/*.parquet'
      )
    '''
    with duckdb.connect('data/BLOCKCHAIN.db') as con:
        con.sql(sql_query)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'blockchain_data_pipeline',
    default_args=default_args,
    description='A simple blockchain data pipeline',
    schedule_interval=timedelta(days=1),
)

start_block = get_last_processed_block() + 1
end_block = start_block + 100

# Define tasks
collect_blocks_task = PythonOperator(
    task_id='collect_blocks',
    python_callable=collect_blocks,
    op_kwargs={'start_block': start_block, 'end_block': end_block},
    dag=dag,
)

collect_logs_task = PythonOperator(
    task_id='collect_logs',
    python_callable=collect_logs,
    op_kwargs={'start_block': start_block, 'end_block': end_block},
    dag=dag,
)

collect_transactions_task = PythonOperator(
    task_id='collect_transactions',
    python_callable=collect_transactions,
    op_kwargs={'start_block': start_block, 'end_block': end_block},
    dag=dag,
)

create_logs_table_task = PythonOperator(
    task_id='create_or_replace_table_logs',
    python_callable=create_or_replace_table_logs,
    dag=dag,
)

create_blocks_table_task = PythonOperator(
    task_id='create_or_replace_table_blocks',
    python_callable=create_or_replace_table_blocks,
    dag=dag,
)

create_transactions_table_task = PythonOperator(
    task_id='create_or_replace_table_transactions',
    python_callable=create_or_replace_table_transactions,
    dag=dag,
)

# Set task dependencies
collect_blocks_task >> collect_logs_task >> collect_transactions_task >> [create_logs_table_task, create_blocks_table_task, create_transactions_table_task]