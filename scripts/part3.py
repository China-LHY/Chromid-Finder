import sys
import os
from Bio import SeqIO
from concurrent.futures import ProcessPoolExecutor
import gzip

# 计算四联体频率
def calculate_tetranucleotide_frequencies(sequence):
    base_freqs = {'A': 0, 'T': 0, 'C': 0, 'G': 0}
    total_tetranucleotides = 0
    tetranucleotides = {}

    # 只统计ATGC的碱基频率，忽略其它字符
    for base in sequence:
        if base in base_freqs:  # 只统计A, T, C, G
            base_freqs[base] += 1
    total_bases = sum(base_freqs.values())

    # 计算四联体频率，只处理ATGC中的四联体
    for i in range(len(sequence) - 3):
        tetranucleotide = sequence[i:i + 4]
        if any(base not in base_freqs for base in tetranucleotide):  # 如果四联体包含非ATGC字符，跳过
            continue
        if tetranucleotide not in tetranucleotides:
            tetranucleotides[tetranucleotide] = 0
        tetranucleotides[tetranucleotide] += 1
        total_tetranucleotides += 1

    # 计算期望频率并返回结果
    expected_freqs = {}
    for pair, count in tetranucleotides.items():
        expected_freq = (base_freqs[pair[0]] / total_bases) * \
                        (base_freqs[pair[1]] / total_bases) * \
                        (base_freqs[pair[2]] / total_bases) * \
                        (base_freqs[pair[3]] / total_bases)
        expected_freqs[pair] = expected_freq

    return {pair: (count / total_tetranucleotides) / expected_freqs[pair] for pair, count in tetranucleotides.items()}

# 处理单条记录
def process_record(record):
    seq_id, sequence = record.id, str(record.seq)
    tetranuc_freqs = calculate_tetranucleotide_frequencies(sequence)
    return seq_id, tetranuc_freqs

# 处理记录块（避免嵌套池）
def process_chunk(chunk):
    return [process_record(record) for record in chunk]

# 流式分块读取器
def stream_fasta_chunks(input_file, chunk_size=100):
    """流式读取FASTA文件，按内存大小分块"""
    chunk = []
    current_size = 0
    max_chunk_size = 1000 * 1024 * 1024  # 100MB/块
    
    open_func = gzip.open if input_file.endswith('.gz') else open
    with open_func(input_file, 'rt') as handle:
        for record in SeqIO.parse(handle, "fasta"):
            rec_size = len(record.seq) + len(record.id) + 100  # 预估内存
            if current_size + rec_size > max_chunk_size and chunk:
                yield chunk
                chunk = []
                current_size = 0
                
            chunk.append(record)
            current_size += rec_size
    
    if chunk:
        yield chunk

# 主函数（完全重写）
def main(input_file, out_file, cpu):
    
    # 立即打开输出文件（流式写入）
    with open(out_file, 'w') as out_f:
        # 创建进程池（单层）
        with ProcessPoolExecutor(max_workers=cpu) as executor:
            # 流式分块提交任务
            futures = []
            for chunk in stream_fasta_chunks(input_file):
                future = executor.submit(process_chunk, chunk)
                futures.append(future)
                
                # 及时回收完成的任务
                while len(futures) >= cpu * 2:  # 控制排队任务数
                    done = [f for f in futures if f.done()]
                    for f in done:
                        for seq_id, freqs in f.result():
                            out_f.write(f"{seq_id}\t{freqs}\n")
                        futures.remove(f)
            
            # 处理剩余任务
            for future in futures:
                for seq_id, freqs in future.result():
                    out_f.write(f"{seq_id}\t{freqs}\n")

if __name__ == "__main__":
    if len(sys.argv) != 4:  # 修改参数数量
        print("用法: python part3.py <input_file> <output_file> <cpu>")
        sys.exit(1)
        
    input_file, out_file, cpu = sys.argv[1], sys.argv[2], int(sys.argv[3])
    main(input_file, out_file, cpu)
