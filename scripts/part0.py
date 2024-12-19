from Bio import SeqIO
import math
import sys

def split_faa(input_file, output_prefix, num_parts):
    records = list(SeqIO.parse(input_file, "fasta"))

    # 计算每个部分的大小
    records_per_part = math.ceil(len(records) / num_parts)

    # 分割并保存每个部分的记录
    for i in range(num_parts):
        start_index = i * records_per_part
        end_index = min((i + 1) * records_per_part, len(records))

        output_file = f"{output_prefix}_part{i + 1}.fasta"
        SeqIO.write(records[start_index:end_index], output_file, "fasta")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <cpu> ")
        sys.exit(1)
    input_file = sys.argv[1]  
    output_prefix = "g"  
    num_parts = int(sys.argv[2])
    split_faa(input_file, output_prefix, num_parts)
