from scripts.bronze import fetch_all_data
from scripts.silver import run_silver
from scripts.gold import run_gold

def run_pipeline():
    fetch_all_data()
    run_silver()
    run_gold()

if __name__ == "__main__":
    run_pipeline()