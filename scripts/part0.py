import os
import sys
import math
from multiprocessing import Process

def build_index(input_file):
    """构建索引：记录每个记录的起始位置和长度（内存友好）"""
    index = []
    with open(input_file, 'rb') as f:
        pos = 0
        while True:
            line = f.readline()
            if not line:
                break
            if line.startswith(b'>'):
                start = pos
                # 读取整个记录直到下一个'>'
                while True:
                    pos = f.tell()
                    next_line = f.readline()
                    if not next_line or next_line.startswith(b'>'):
                        break
                index.append((start, pos - start))
                if next_line:
                    f.seek(pos)  # 回退到上一个位置
            else:
                pos = f.tell()
    return index

def split_faa(input_file, output_prefix, num_parts, num_processes):
    # 1. 构建索引（单次遍历）
    index = build_index(input_file)
    total_records = len(index)
    
    # 2. 分配记录到不同part
    records_per_part = math.ceil(total_records / num_parts)
    
    # 3. 启动多进程处理
    processes = []
    part_per_process = math.ceil(num_parts / num_processes)
    
    for proc_id in range(num_processes):
        start_part = proc_id * part_per_process
        end_part = min((proc_id + 1) * part_per_process, num_parts)
        p = Process(
            target=process_part,
            args=(input_file, output_prefix, index, 
                 start_part + 1, end_part, records_per_part)
        )
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()

def process_part(input_file, output_prefix, index, start_part, end_part, records_per_part):
    """处理指定范围的part"""
    with open(input_file, 'rb') as src_f:
        for part_num in range(start_part, end_part + 1):
            output_file = f"{output_prefix}_part{part_num}.fasta"
            start_idx = (part_num - 1) * records_per_part
            end_idx = min(part_num * records_per_part, len(index))
            
            with open(output_file, 'wb') as dst_f:
                for rec_id in range(start_idx, end_idx):
                    start_pos, length = index[rec_id]
                    src_f.seek(start_pos)
                    dst_f.write(src_f.read(length))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <cpu>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    num_processes = int(sys.argv[2])
    output_prefix = "g"
    
    split_faa(input_file, output_prefix, num_processes, num_processes)
