import subprocess

scripts = [
    "bronze.py",
    "silver.py",
    "silver_quality.py",
    "gold.py",
    "gold_quality.py"
]

for script in scripts:
    print(f"\nRunning {script}...\n")

    result = subprocess.run(
        ["python", script],
        check=True
    )

print("\nPipeline completed.")