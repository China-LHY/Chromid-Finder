import sys
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed  # 修复导入问题
import os

def load_and_preprocess_data(gc_file, intput_file):
    """Load and preprocess the input data."""
    # Load gc.tsv and ensure numeric data
    gc_df = pd.read_csv(gc_file, sep='\t', header=None, names=['id', 'length', 'GC'])
    gc_df['GC'] = pd.to_numeric(gc_df['GC'], errors='coerce')
    gc_df['length'] = pd.to_numeric(gc_df['length'], errors='coerce')
    gc_df.dropna(subset=['GC', 'length'], inplace=True)

    # Load prescreen.tsv
    output_df = pd.read_csv(intput_file, sep='\t', header=None, names=['sequence_id', 'prefixes'])

    # Merge dataframes on sequence_id and id
    merged_df = output_df.merge(gc_df, left_on='sequence_id', right_on='id', how='left')

    # Filter rows with 'dnaa' prefix
    dnaa_df = merged_df[merged_df['prefixes'].str.contains('dnaa', na=False)]
    
    # 添加索引以加速查找
    merged_df.set_index('sequence_id', inplace=True)
    merged_df.sort_values(by='GC', inplace=True)
    gc_values = merged_df['GC'].values

    return dnaa_df, merged_df, gc_values

def find_clusters(dnaa_row, merged_df, gc_values, gc_threshold=1):
    """Find clusters for a given dnaa row using optimized search."""
    cluster = [dnaa_row['sequence_id']]
    dnaa_gc = dnaa_row['GC']
    dnaa_length = dnaa_row['length']
    
    # 使用二分查找快速定位GC范围
    left_idx = np.searchsorted(gc_values, dnaa_gc - gc_threshold, side='left')
    right_idx = np.searchsorted(gc_values, dnaa_gc + gc_threshold, side='right')
    
    # 获取候选序列
    candidates = merged_df.iloc[left_idx:right_idx]
    # 应用长度过滤
    valid_sequences = candidates[
        (candidates['length'] < dnaa_length) & 
        (candidates.index != dnaa_row['sequence_id'])
    ]
    
    cluster.extend(valid_sequences.index.tolist())
    return cluster

def process_batch(dnaa_batch, merged_df, gc_values):
    """处理一批dnaa序列的辅助函数"""
    return [find_clusters(row, merged_df, gc_values) for _, row in dnaa_batch.iterrows()]

def cluster_sequences(gc_file, intput_file, part4_file):
    """Main logic to cluster sequences with parallel processing."""
    dnaa_df, merged_df, gc_values = load_and_preprocess_data(gc_file, intput_file)
    
    clusters = []
    num_processes = os.cpu_count() or 4
    
    # 如果dnaa序列数量少，直接单进程处理
    if len(dnaa_df) < 50:
        for _, row in dnaa_df.iterrows():
            clusters.append(find_clusters(row, merged_df, gc_values))
    else:
        # 并行处理：将dnaa序列分成批次
        batch_size = max(1, len(dnaa_df) // num_processes)
        batches = []
        for i in range(0, len(dnaa_df), batch_size):
            batches.append(dnaa_df.iloc[i:i+batch_size])
        
        with ProcessPoolExecutor(max_workers=cpu) as executor:
            futures = []
            for batch in batches:
                # 每个批次提交一个任务
                future = executor.submit(
                    process_batch, 
                    batch,
                    merged_df,
                    gc_values
                )
                futures.append(future)
            
            # 修复这里：使用直接导入的 as_completed
            for future in as_completed(futures):
                clusters.extend(future.result())

    # 写入文件
    with open(part4_file, "w") as f:
        for cluster in clusters:
            f.write(f"Central sequence in the cluster: {cluster[0]}\n")
            f.write(f"Other sequences in the cluster: {', '.join(cluster[1:])}\n")
            f.write("------\n")
    print(f"Clustered data written to {part4_file}")

def main(gc_file, intput_file, part4_file, cpu):
    cluster_sequences(gc_file, intput_file, part4_file)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <cpu>" )
        sys.exit(1)

    gc_file = "gc.tsv"
    intput_file = "part2.txt"
    part4_file = "part4.txt"
    cpu = int(sys.argv[1])

    main(gc_file, intput_file, part4_file,cpu)
