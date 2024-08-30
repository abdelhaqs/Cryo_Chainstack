import os
import cryo
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL and PostgreSQL connection details from environment variables
eth_rpc = os.getenv("ETH_RPC")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

print(f"ETH_RPC: {eth_rpc}")
print(f"DB_HOST: {db_host}")
print(f"DB_NAME: {db_name}")
print(f"DB_USER: {db_user}")
print(f"DB_PASSWORD: {db_password}")

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

def create_schema_if_not_exists(engine, schema_name):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        conn.commit()  # Ensure the schema creation is committed

def collect_and_insert_blocks(engine, start_block, end_block):
    # Collect blockchain data using the cryo library and return it as a pandas DataFrame
    blocks_data = cryo.freeze(
    "blocks",
    blocks=[f"{start_block}:{end_block}"], 
    rpc=eth_rpc,
    output_dir="raw/blocks",
    file_format="parquet",
    #file_name="file_name",
    hex=True,
    requests_per_second=50
    )

def collect_and_insert_logs(engine, start_block, end_block):
    # Collect logs data using the cryo library and return it as a pandas DataFrame
    logs_data = cryo.freeze(
        "logs", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/logs",
        file_format="parquet",
        #file_name="file_name",
        hex=True,
        requests_per_second=50
    )
    
def collect_and_insert_transactions(engine, start_block, end_block):
    # Collect blockchain data using the cryo library and return it as a pandas DataFrame
    transactions_data = cryo.freeze(
        "transactions", 
        blocks=[f"{start_block}:{end_block}"], 
        rpc=eth_rpc, 
        output_dir="raw/transactions",
        file_format="parquet",
        #file_name="file_name",
        hex=True,
        requests_per_second=50
    )

def main():
    # Create SQLAlchemy engine for PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}')

    # Get the last processed block number
    last_processed_block = get_last_processed_block()

    # Define the range for the new blocks to process
    start_block = last_processed_block + 1
    end_block = start_block + 100  # Adjust the range as needed

    # Collect and insert new blocks
    collect_and_insert_blocks(engine, start_block, end_block)
    
    # Collect and insert logs
    collect_and_insert_logs(engine, start_block, end_block)

    # Collect and insert transactions
    collect_and_insert_transactions(engine, start_block, end_block)

    # Update the last processed block number
    set_last_processed_block(end_block)

if __name__ == "__main__":
    main()
