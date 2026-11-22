import kline_batch_job
import signal_batch_job

if __name__ == "__main__":
    print(f"\n{'='*50}")
    print("--- Starting kline_batch_job ---")
    print(f"{'='*50}")
    kline_batch_job.ensure_tables()
    kline_batch_job.process_klines()
    print("--- kline_batch_job SUCCEEDED ---\n")
    
    print(f"{'='*50}")
    print("--- Starting signal_batch_job ---")
    print(f"{'='*50}")
    signal_batch_job.ensure_signals_table()
    signal_batch_job.process_signals()
    kline_batch_job.spark.stop()
    print("--- signal_batch_job SUCCEEDED ---\n")
