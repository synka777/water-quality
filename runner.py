from ingest import fetch_all_data
from silver import run_silver
from gold import run_gold

def run_pipeline():
    fetch_all_data()
    run_silver()
    run_gold()

if __name__ == "__main__":
    run_pipeline()