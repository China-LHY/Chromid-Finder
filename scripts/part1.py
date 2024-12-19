import subprocess
import sys
from pathlib import Path

def execute_command(command):
    """Executes a shell command and handles errors."""
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}\n{e}")
        sys.exit(1)

def read_lines(file_path):
    """Reads all lines from a file."""
    try:
        with open(file_path, "r") as file:
            return file.readlines()
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        sys.exit(1)

def write_lines(file_path, lines, mode="a"):
    """Writes lines to a file."""
    with open(file_path, mode) as file:
        file.writelines(lines)

def filter_lines(lines, prefix, score_index, threshold, alt_score_index=None):
    """Filters lines based on score and prefix rules."""
    filtered_lines = []
    for line in lines:
        if line.startswith("#"):
            filtered_lines.append(line)
            continue

        columns = line.split()
        if alt_score_index is not None and line.startswith("*"):
            score_index = alt_score_index

        try:
            if len(columns) > score_index and float(columns[score_index]) >= threshold:
                filtered_lines.append(f"{prefix}_{line}")
        except ValueError:
            filtered_lines.append(f"{prefix}_{line}")

    return filtered_lines

def process_file(file_path, output_file, prefix, score_index, threshold=30, alt_score_index=None):
    """Filters and writes data from an input file to an output file."""
    lines = read_lines(file_path)
    filtered_lines = filter_lines(lines, prefix, score_index, threshold, alt_score_index)
    write_lines(output_file, filtered_lines)

def main(input_file):
    # Define commands
    commands = [
        f"prodigal -i {input_file} -a {input_file}.faa -p meta",
        f"hmmsearch --noali --cut_ga --cpu 2 --domtblout {input_file}-core1.out databases/core1.hmm {input_file}.faa",
        f"hmmsearch -Z 1 --noali --cut_ga --cpu 2 --domtblout {input_file}-core2.out databases/core2.hmm {input_file}.faa",
        f"cat {input_file}-core1.out {input_file}-core2.out > merged_{input_file}-core.out",
        f"hmmsearch -Z 1 --noali --cut_ga --cpu 2 --domtblout {input_file}-par1.out databases/par1.hmm {input_file}.faa",
        f"hmmsearch -Z 1 --noali --domE 1e-5 --cpu 2 --domtblout {input_file}-par2.out databases/par2.hmm {input_file}.faa",
        f"cat {input_file}-par1.out {input_file}-par2.out > merged_{input_file}-par.out",
        f"hmmsearch -Z 1 --noali --cut_ga --cpu 2 --domtblout {input_file}-rep1.out databases/rep1.hmm {input_file}.faa",
        f"hmmsearch -Z 1 --noali --domE 1e-5 --cpu 2 --domtblout {input_file}-rep2.out databases/rep2.hmm {input_file}.faa",
        f"cat {input_file}-rep1.out {input_file}-rep2.out > merged_{input_file}-rep.out",
        f"exec_annotation -o {input_file}-dnaA.tsv -p databases/dnaa.hal -k databases/ko_list --cpu 2 -f detail {input_file}.faa"
    ]

    # Execute commands
    for command in commands:
        execute_command(command)

    # Filter DNA data
    dnaA_file = f"{input_file}-dnaA.tsv"
    dnaA_output = f"filtered_{input_file}-dnaA.tsv"
    dnaA_lines = read_lines(dnaA_file)
    filtered_dnaA = filter_lines(dnaA_lines, "dnaa", 3, 100, alt_score_index=4)
    write_lines(dnaA_output, filtered_dnaA, mode="w")

    # Process merged files
    merged_files = [
        (f"merged_{input_file}-core.out", "core", 7),
        (f"merged_{input_file}-par.out", "par", 7),
        (f"merged_{input_file}-rep.out", "rep", 7)
    ]
    output_file = f"{input_file}-part1.txt"

    if Path(output_file).exists():
        Path(output_file).unlink()

    for file_path, prefix, score_index in merged_files:
        process_file(file_path, output_file, prefix, score_index)

    # Append filtered DNA data to the output file
    dnaA_filtered_lines = read_lines(dnaA_output)
    write_lines(output_file, dnaA_filtered_lines)

    print(f"Processing complete. Final output written to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file>")
        sys.exit(1)

    main(sys.argv[1])
