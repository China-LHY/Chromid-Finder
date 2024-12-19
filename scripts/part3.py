import sys
from Bio import SeqIO
from concurrent.futures import ProcessPoolExecutor, as_completed

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

# 处理多个记录
def process_file_chunk(records, cpu):
    results = []
    with ProcessPoolExecutor(max_workers=cpu) as executor:
        futures = [executor.submit(process_record, record) for record in records]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"处理记录时出错: {e}")
    return results

# 写入结果到文件
def write_results_to_file(results, out_file):
    with open(out_file, "w") as outfile:
        for seq_id, tetranuc_freqs in results:
            outfile.write(f"{seq_id}\t{tetranuc_freqs}\n")

# 主函数
def main(input_file, out_file, cpu):
    batch_size = 5000
    chunks = []

    # 读取文件并分块
    with open(input_file) as handle:
        records = []
        for record in SeqIO.parse(handle, "fasta"):
            records.append(record)
            if len(records) >= batch_size:
                chunks.append(records)
                records = []
        if records:
            chunks.append(records)

    # 处理分块并合并结果
    results = []
    with ProcessPoolExecutor(max_workers=cpu) as executor:
        futures = [executor.submit(process_file_chunk, chunk, cpu) for chunk in chunks]
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"处理分块时出错: {e}")

    # 写入处理结果
    write_results_to_file(results, out_file)

# 主程序入口
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python process_fasta.py <input_file> <cpu>")
        sys.exit(1)

    input_file = sys.argv[1]
    cpu = int(sys.argv[2])
    out_file = "part3.txt"
    main(input_file, out_file, cpu)
