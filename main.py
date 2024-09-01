import os
import time
import cryo
from filelock import FileLock
from dotenv import load_dotenv
import duckdb
import pandas as pd
from dagster import op, job, schedule, repository, In, Out

# Load environment variables from .env file
load_dotenv()

eth_rpc = os.getenv("ETH_RPC")

LAST_PROCESSED_BLOCK_FILE = 'last_processed_block.txt'
LOCK_FILE = 'data/blockchain.db.lock'
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds

def acquire_lock(lock_file):
    """Acquire a file lock to ensure exclusive access."""
    lock = FileLock(lock_file)
    lock.acquire()
    return lock

def release_lock(lock):
    """Release the file lock."""
    lock.release()

def execute_query_with_retry(sql_query, db_path, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    """Execute a query with retry logic if the database is already open."""
    attempt = 0
    while attempt < max_retries:
        try:
            with duckdb.connect(db_path) as con:
                con.sql(sql_query)
            break  # Exit loop if successful
        except duckdb.IOException as e:
            if 'File is already open' in str(e):
                attempt += 1
                print(f"Attempt {attempt} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise e  # Rethrow other exceptions
    else:
        raise RuntimeError(f"Failed to execute query after {max_retries} attempts.")

@op(out=Out(int))
def get_last_processed_block_op():
    if os.path.exists(LAST_PROCESSED_BLOCK_FILE):
        with open(LAST_PROCESSED_BLOCK_FILE, 'r') as file:
            return int(file.read().strip())
    else:
        return 0

@op(out=Out(int))
def calculate_start_block_op(last_processed_block: int):
    return last_processed_block + 1

@op(out=Out(int))
def calculate_end_block_op(start_block: int):
    return start_block + 100

@op
def set_last_processed_block_op(block_number: int):
    with open(LAST_PROCESSED_BLOCK_FILE, 'w') as file:
        file.write(str(block_number))

@op
def collect_blocks_op(start_block: int, end_block: int):
    cryo.freeze(
        "blocks",
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc,
        output_dir="raw/blocks",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

@op
def collect_logs_op(start_block: int, end_block: int):
    cryo.freeze(
        "logs", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/logs",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

@op
def collect_transactions_op(start_block: int, end_block: int):
    cryo.freeze(
        "transactions", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/transactions",
        file_format="parquet",
        hex=True,
        requests_per_second=50
    )

@op
def create_or_replace_table_logs_op():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_logs AS
    SELECT *
    FROM read_parquet(
      'raw/logs/*.parquet'
      )
    '''
    execute_query_with_retry(sql_query, 'data/BLOCKCHAIN.db')

@op
def create_or_replace_table_blocks_op():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_blocks AS
    SELECT *
    FROM read_parquet(
      'raw/blocks/*.parquet'
      )
    '''
    execute_query_with_retry(sql_query, 'data/BLOCKCHAIN.db')

@op
def create_or_replace_table_transactions_op():
    sql_query = '''
    CREATE OR REPLACE TABLE optimism_sepolia_transactions AS
    SELECT *
    FROM read_parquet(
      'raw/transactions/*.parquet'
      )
    '''
    execute_query_with_retry(sql_query, 'data/BLOCKCHAIN.db')

@job
def blockchain_etl_job():
    last_processed_block = get_last_processed_block_op()
    start_block = calculate_start_block_op(last_processed_block)
    end_block = calculate_end_block_op(start_block)

    collect_blocks_op(start_block, end_block)
    collect_logs_op(start_block, end_block)
    collect_transactions_op(start_block, end_block)
    set_last_processed_block_op(end_block)

    create_or_replace_table_logs_op()
    create_or_replace_table_blocks_op()
    create_or_replace_table_transactions_op()

@schedule(cron_schedule="*/18 * * * *", job=blockchain_etl_job, execution_timezone="UTC")
def blockchain_etl_schedule():
    return {}

@repository
def blockchain_etl_repo():
    return [blockchain_etl_job, blockchain_etl_schedule]