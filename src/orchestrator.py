import time
import subprocess
import sys
import os
from dotenv import load_dotenv

# Force load all variables from .env so subprocesses inherit them
load_dotenv()

def run_pipeline():
    print(f"\n--- Starting Orchestration Cycle: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    print("Running Ingestion...")
    subprocess.run([sys.executable, "-m", "src.warehouse.ingestion"], check=True)
    
    print("Running dbt Transformations...")
    subprocess.run(["dbt", "run", "--profiles-dir", "."], cwd="src/transformations", check=True, shell=True)
    
    print("Cycle Complete. Sleeping for 60 seconds...")

if __name__ == "__main__":
    print("Starting Continuous IoT Data Warehouse Pipeline...")
    try:
        while True:
            run_pipeline()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nPipeline stopped by user.")