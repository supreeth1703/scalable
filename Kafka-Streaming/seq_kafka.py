from kafka import KafkaConsumer
import json
import time
import psutil
import os
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import pandas as pd
import numpy as np
import boto3  # added for S3 upload

# Config
BROKER = 'localhost:9092'
TOPIC = 'arxiv-stream'
WORKLOADS = [10000, 30000, 60000, 90000]
INTERVAL = 50000

# S3 Config
BUCKET_NAME = 's3-x24108863'
S3_OUTPUT_PREFIX = 'outputs/'

# Create result directory
os.makedirs('sequential_arxiv_results', exist_ok=True)

# Boto3 S3 client
s3_client = boto3.client('s3')

# Upload function
def upload_to_s3(local_file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"--> Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("*** Kafka consumer connected. Streaming arXiv messages...")

metrics_records = []
final_category = Counter()
final_journal = Counter()
final_words = 0

for workload in WORKLOADS:
    print(f"\n========== Processing Workload {workload} ==========")

    category_count = Counter()
    journal_versions = Counter()
    total_words = 0

    record_count = 0
    workload_start_time = time.time()
    interval_start_time = time.time()

    while record_count < workload:
        message = next(consumer)
        record = message.value

        # Category distribution
        category = record.get('categories', 'Unknown')
        category_count.update(category.split())

        # Journal version count
        journal_ref = record.get('journal-ref')
        if journal_ref:
            journal_versions[journal_ref] += int(len(record.get('versions', [{}])))

        # Abstract word count
        abstract = record.get('abstract', '')
        total_words += len(abstract.split())

        record_count += 1

        if record_count % INTERVAL == 0 or record_count == workload:

            batch_time = time.time() - interval_start_time
            throughput = INTERVAL / batch_time
            cpu_usage = psutil.cpu_percent(interval=None)
            mem_usage = psutil.virtual_memory().percent

            print(f"Processed {record_count} records — Batch Time: {batch_time:.2f}s, "
                  f"Throughput: {throughput:.2f}/sec, CPU: {cpu_usage}%, Mem: {mem_usage}%")

            metrics_records.append({
                'workload': workload,
                'records_processed': record_count,
                'time': batch_time,
                'throughput': throughput,
                'cpu': cpu_usage,
                'memory': mem_usage
            })

            interval_start_time = time.time()

    workload_total_time = time.time() - workload_start_time

    print(f"\n======> Final Summary for Workload {workload}:")
    print(f"   Total Records Processed: {workload}")
    print(f"   Total Processing Time: {workload_total_time:.2f} seconds")

    final_category.update(category_count)
    final_journal.update(journal_versions)
    final_words += total_words

consumer.close()

# Print final task results
print("\n*** Final Task Results Across All Workloads:")
print(f"Top 5 Categories: {final_category.most_common(5)}")
print(f"Top 5 Journals by Versions: {final_journal.most_common(5)}")
print(f"Total Abstract Word Count: {final_words}")

# Convert metrics to DataFrame
metrics_df = pd.DataFrame(metrics_records)

# Plot and upload bar graph
def plot_metrics_bar(df):
    sns.set(style='whitegrid')
    os.makedirs('results', exist_ok=True)
    palette = sns.color_palette("Set2", n_colors=len(WORKLOADS))

    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    metric_names = ['Time (s)', 'Throughput (records/sec)', 'CPU (%)', 'Memory (%)']
    columns = ['time', 'throughput', 'cpu', 'memory']

    # Aggregate averages per workload
    agg_df = df.groupby('workload')[columns].mean().reset_index()

    for ax, col, name in zip(axes.flatten(), columns, metric_names):
        sns.barplot(
            data=agg_df,
            x='workload',
            y=col,
            palette=palette,
            ax=ax
        )
        ax.set_title(f"{name} vs Workload (Sequential Stream)", fontsize=16)
        ax.set_xlabel("Workload (records)", fontsize=14)
        ax.set_ylabel(name, fontsize=14)
        ax.grid(True)

    plt.tight_layout()

    local_file_path = 'results/arxiv_sequential_bar_metrics.png'
    plt.savefig(local_file_path)
    plt.close()

    # Upload to S3
    s3_key = f"{S3_OUTPUT_PREFIX}arxiv_sequential_bar_metrics.png"
    upload_to_s3(local_file_path, BUCKET_NAME, s3_key)

plot_metrics_bar(metrics_df)
print("\n*** Combined bar graphs plotted and uploaded to S3.")
