import json
import time
from collections import Counter
from multiprocessing import cpu_count
import psutil
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3  # <--- Added

# === CONFIG ===
DATA_FILE = '/home/ec2-user/environment/arxiv-metadata-oai-snapshot.json'
CHUNK_SIZE = 100000
MAX_RECORDS = 2000000
WORKLOADS = [100000, 300000, 600000, 900000]
BUCKET_NAME = 's3-x24108863'
S3_OUTPUT_PREFIX = 'outputs/'

# === Boto3 S3 Client ===
s3_client = boto3.client('s3')

# === UPLOAD FUNCTION ===
def upload_to_s3(local_file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"--> Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# === MAPPER FUNCTION ===
def mapper(chunk):
    category_counter = Counter()
    total_abstract_words = 0
    journal_versions = Counter()

    for record in chunk:
        category = record.get('categories', '').split()[0]
        category_counter[category] += 1

        abstract_words = len(record.get('abstract', '').split())
        total_abstract_words += abstract_words

        journal_ref = record.get('journal-ref', 'Unknown')
        versions = len(record.get('versions', []))
        journal_versions[journal_ref] += versions

    return category_counter, total_abstract_words, journal_versions

# === READ JSON IN CHUNKS ===
def read_in_chunks(file_path, chunk_size, max_records):
    with open(file_path, 'r', encoding='utf-8') as f:
        chunk = []
        total_records = 0
        for line in f:
            record = json.loads(line)
            chunk.append(record)
            total_records += 1
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
            if total_records >= max_records:
                break
        if chunk:
            yield chunk

# === REDUCE FUNCTION ===
def reducer(results):
    total_category = Counter()
    total_abstract_words = 0
    total_journal_versions = Counter()

    for result in results:
        cat_count, abstract_words, journal_vers = result
        total_category.update(cat_count)
        total_abstract_words += abstract_words
        total_journal_versions.update(journal_vers)

    return total_category, total_abstract_words, total_journal_versions

# === SEQUENTIAL EXECUTION ===
def run_sequential(max_records):
    process = psutil.Process()
    mem_before = process.memory_info().rss / (1024 ** 2)

    start_time = time.time()
    cpu_usage_before = psutil.cpu_percent(interval=None)

    results = []
    for chunk in read_in_chunks(DATA_FILE, CHUNK_SIZE, max_records):
        result = mapper(chunk)
        results.append(result)

    elapsed = time.time() - start_time
    cpu_usage = psutil.cpu_percent(interval=elapsed)
    mem_after = process.memory_info().rss / (1024 ** 2)

    reducer(results)
    time_taken = elapsed
    memory_used = mem_after - mem_before
    throughput = (len(results) * CHUNK_SIZE) / time_taken

    return time_taken, memory_used, cpu_usage, throughput

# === PARALLEL EXECUTION (ThreadPoolExecutor) ===
def run_parallel(max_records):
    process = psutil.Process()
    mem_before = process.memory_info().rss / (1024 ** 2)

    start_time = time.time()
    cpu_usage_before = psutil.cpu_percent(interval=None)

    results = []
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [executor.submit(mapper, chunk) for chunk in read_in_chunks(DATA_FILE, CHUNK_SIZE, max_records)]
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.time() - start_time
    cpu_usage = psutil.cpu_percent(interval=elapsed)
    mem_after = process.memory_info().rss / (1024 ** 2)

    reducer(results)
    time_taken = elapsed
    memory_used = mem_after - mem_before
    throughput = (len(results) * CHUNK_SIZE) / time_taken

    return time_taken, memory_used, cpu_usage, throughput

# === PLOTTING RESULTS ===
def plot_all_metrics(seq_metrics_list, par_metrics_list, workloads):
    labels = ['Time (s)', 'Memory (MB)', 'CPU (%)', 'Throughput (records/s)']
    num_metrics = len(labels)
    x = range(len(workloads))

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()

    for i in range(num_metrics):
        ax = axes[i]
        seq_values = [metrics[i] for metrics in seq_metrics_list]
        par_values = [metrics[i] for metrics in par_metrics_list]

        bar_width = 0.35
        ax.bar([p - bar_width/2 for p in x], seq_values, width=bar_width, label='Sequential')
        ax.bar([p + bar_width/2 for p in x], par_values, width=bar_width, label='Parallel')

        ax.set_xticks(x)
        ax.set_xticklabels([f'{w//1000}K' for w in workloads])
        ax.set_xlabel('Workload Size (records)')
        ax.set_ylabel(labels[i])
        ax.set_title(f'{labels[i]} vs Workload Size')
        ax.legend()
        ax.grid(axis='y', linestyle='--', alpha=0.6)

    plt.suptitle('Sequential vs Parallel MapReduce Performance (Multiple Workloads)', fontsize=16)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save graph locally
    local_file_path = 'MR_performance_all_metrics.png'
    plt.savefig(local_file_path)
    plt.close()

    # Upload graph to S3
    s3_key = f'{S3_OUTPUT_PREFIX}performance_all_metrics5.png'
    upload_to_s3(local_file_path, BUCKET_NAME, s3_key)

# === MAIN FUNCTION ===
def main():
    seq_metrics_list = []
    par_metrics_list = []

    seq_total_time = seq_total_mem = seq_total_cpu = seq_total_tp = 0
    par_total_time = par_total_mem = par_total_cpu = par_total_tp = 0

    for w in WORKLOADS:
        print(f"\n==== Sequential processing for {w} records ====")
        seq_metrics = run_sequential(w)
        seq_metrics_list.append(seq_metrics)
        seq_total_time += seq_metrics[0]
        seq_total_mem += seq_metrics[1]
        seq_total_cpu += seq_metrics[2]
        seq_total_tp += seq_metrics[3]
        print(f"Sequential metrics for {w}: {seq_metrics}")
        print(f"Sequential cumulative so far -> Time: {seq_total_time:.2f}s, Memory: {seq_total_mem:.2f}MB, CPU: {seq_total_cpu:.2f}%, Throughput: {seq_total_tp:.2f} records/s")

        print(f"\n==== Parallel processing for {w} records ====")
        par_metrics = run_parallel(w)
        par_metrics_list.append(par_metrics)
        par_total_time += par_metrics[0]
        par_total_mem += par_metrics[1]
        par_total_cpu += par_metrics[2]
        par_total_tp += par_metrics[3]
        print(f"Parallel metrics for {w}: {par_metrics}")
        print(f"Parallel cumulative so far -> Time: {par_total_time:.2f}s, Memory: {par_total_mem:.2f}MB, CPU: {par_total_cpu:.2f}%, Throughput: {par_total_tp:.2f} records/s")

    print("\n==== Final Cumulative Totals ====")
    print(f"Sequential Total Time: {seq_total_time:.2f}s")
    print(f"Parallel   Total Time: {par_total_time:.2f}s")

    if par_total_time < seq_total_time:
        print("\nParallel total execution time is less than Sequential — as expected.")
    else:
        print("\nParallel total execution time is not less than Sequential. Investigate workload size, CPU count, or overhead.")

    plot_all_metrics(seq_metrics_list, par_metrics_list, WORKLOADS)
    print("\nAll performance graphs saved and uploaded to S3.")

if __name__ == "__main__":
    main()
