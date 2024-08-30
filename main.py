import os
import cryo
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL and PostgreSQL connection details from environment variables
eth_rpc = os.getenv("ETH_RPC")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

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

def collect_blocks(start_block, end_block):
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

def collect_logs(start_block, end_block):
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
    
def collect_transactions(start_block, end_block):
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
    # Get the last processed block number
    last_processed_block = get_last_processed_block()

    # Define the range for the new blocks to process
    start_block = last_processed_block + 1
    end_block = start_block + 100  # Adjust the range as needed

    # Collect new blocks
    collect_blocks(start_block, end_block)
    
    # Collect logs
    collect_logs(start_block, end_block)

    # Collect transactions
    collect_transactions(start_block, end_block)

    # Update the last processed block number
    set_last_processed_block(end_block)

if __name__ == "__main__":
    main()
