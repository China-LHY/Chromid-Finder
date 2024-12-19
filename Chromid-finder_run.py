import os
import subprocess
import argparse
import glob
import shutil
from multiprocessing import Pool
from Bio import SeqIO

# 定义脚本路径
SCRIPT_0 = "scripts/part0.py"
SCRIPT_1 = "scripts/part1.py"
SCRIPT_2 = "scripts/part2.py"
SCRIPT_3 = "scripts/part3.py"
SCRIPT_4 = "scripts/part4.py"
SCRIPT_5 = "scripts/part5.py"

# 生成gc.tsv文件
def generate_gc_file(input_file):
    command = f"seqkit fx2tab -l -g -n -i -H {input_file} > gc.tsv"
    run_command(command)

# 执行命令行命令
def run_command(command):
    print(f"Running command: {command}")
    subprocess.run(command, shell=True, check=True)

# 判断文件大小是否超过5GB
def is_large_file(file_path, size_limit=5 * 1024**3):
    return os.path.getsize(file_path) > size_limit

# 调用0.py
def run_script_0(input_file, cpu):
    command = f"python {SCRIPT_0} {input_file} {cpu}"
    run_command(command)

# 调用1.py
def run_script_1(input_file):
    command = f"python {SCRIPT_1} {input_file}"
    run_command(command)

# 调用2.py
def run_script_2():
    command = f"python {SCRIPT_2} "
    run_command(command)

# 调用3.py
def run_script_3(input_file, cpu):
    command = f"python {SCRIPT_3} {input_file} {cpu}"
    run_command(command)

# 调用4.py
def run_script_4():
    command = f"python {SCRIPT_4} "
    run_command(command)

# 调用5.py
def run_script_5(output_file, cpu, dt):
    command = f"python {SCRIPT_5} {output_file} {cpu} {dt}"
    run_command(command)
    


# 删除不需要的文件和目录
def clean_up_files(output_file, input_file):
    current_directory = os.getcwd()

    for item in os.listdir(current_directory):
        item_path = os.path.join(current_directory, item)
        # 排除 scripts 目录、databases 目录、Chromid-finder_run.py 脚本以及最终输出文件
        if item in ["scripts", "databases", "Chromid-finder_run.py"] or item == output_file or item == input_file:
            continue

        if os.path.isfile(item_path):
            os.remove(item_path)
            print(f"Deleted file: {item_path}")
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
            print(f"Deleted directory and its contents: {item_path}")

# 处理大文件的1.py并行运行
def process_large_file(input_file, cpu, output_file, dt):
    # 首先运行0.py
    run_script_0(input_file, cpu)
    
    # 获取0.py的输出文件
    gpartfiles = glob.glob("g_part*.fasta")
    print(f"gpartfiles: {gpartfiles}")
    # 使用多进程并行调用1.py
    with Pool(cpu) as pool:
        pool.map(run_script_1, gpartfiles)
    
    # 合并1.py的输出文件
    with open("part1.txt", "w") as part1:
        for file in glob.glob("g_part*-part1.txt"):
            with open(file, "r") as f:
                shutil.copyfileobj(f, part1)

    # 继续调用其他脚本
    run_script_2()
    run_script_3(input_file, cpu)
    run_script_4()
    run_script_5(output_file, cpu, dt)


# 处理小文件的顺序运行
def process_small_file(input_file, cpu, output_file, dt):
    # 直接调用1.py并重命名输出
    run_script_1(input_file)

    # 获取1.py的输出文件，假设输出文件的格式为 "{input_file}-part1.txt"
    output_1_file = f"{input_file}-part1.txt"
    
    # 检查文件是否存在
    if os.path.exists(output_1_file):
        # 重命名输出文件为 part1.txt
        os.rename(output_1_file, "part1.txt")
    else:
        print(f"Error: Expected file {output_1_file} not found.")
        return
    
    # 依次调用其他脚本
    run_script_2()
    run_script_3(input_file, cpu)
    run_script_4()
    run_script_5(output_file,cpu,dt)

# 主函数逻辑
def run_chromid_finder(input_file, cpu, output_file, dt):
    # 生成gc.tsv文件
    generate_gc_file(input_file)

    # 判断文件大小

    if is_large_file(input_file):
        print(f"Input file is large, size > 5GB. Running scripts with parallel processing.")
        process_large_file(input_file, cpu, output_file, dt)
    else:
        print(f"Input file is small, size < 5GB. Running scripts sequentially.")
        process_small_file(input_file, cpu, output_file, dt)
    
    # 清理中间文件
    clean_up_files(output_file, input_file)
    print("Process completed.")

# 命令行解析函数
def parse_args():
    parser = argparse.ArgumentParser(description="Run Chromid-finder pipeline")
    parser.add_argument('-i', '--input', required=True, help="Input fasta file")
    parser.add_argument('-n', '--cpu', type=int, required=True, help="Number of CPUs")
    parser.add_argument('-o', '--output', required=True, help="Output file")
    parser.add_argument('-d', '--dt', type=float, required=True, help="Parameter for dt")
    return parser.parse_args()

# 主入口
if __name__ == '__main__':
    args = parse_args()
    run_chromid_finder(args.input, args.cpu, args.output, args.dt)
