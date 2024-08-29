import os
import cryo
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
eth_rpc = os.getenv("ETH_RPC")

output_dir = "data/raw"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Collect blockchain data using the cryo library and save it directly as a CSV
cryo.collect(
    "blocks", 
    blocks=["405:410"], 
    rpc=eth_rpc, 
    output_dir=output_dir,  # Specify the output directory
    csv=True,               # Specify CSV output format
    hex=True
)

# Inform the user that the data has been saved
print(f"Blockchain data has been collected and saved in {output_dir} as a CSV file.")

