import subprocess
import sys

def run_job(script_name):
    print(f"\n{'='*50}")
    print(f"--- Starting {script_name} ---")
    print(f"{'='*50}")
    result = subprocess.run([sys.executable, script_name])
    if result.returncode != 0:
        print(f"!!! {script_name} FAILED with exit code {result.returncode} !!!")
        sys.exit(result.returncode)
    print(f"--- {script_name} SUCCEEDED ---\n")

if __name__ == "__main__":
    run_job("kline_batch_job.py")
    run_job("signal_batch_job.py")
