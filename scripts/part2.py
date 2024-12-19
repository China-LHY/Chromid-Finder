import sys
from collections import defaultdict

def clean_file(input_file, output_file):
    try:
        with open(input_file, "r") as infile, open(output_file, "w") as outfile:
            for line in infile:
                if line.startswith("#") or not line.strip():
                    # Write comments and empty lines as-is
                    outfile.write(line)
                    continue

                parts = line.strip().split()
                if len(parts) < 2:
                    # Write lines with insufficient columns as-is
                    outfile.write(line)
                    continue

                if parts[0].startswith("dnaa"):
                    # Clean 'dnaa' prefix and combine with second column
                    cleaned_prefix = parts[0].replace("*", "") + parts[1]
                    remaining = " ".join(parts[2:])
                    outfile.write(f"{cleaned_prefix} {remaining}\n")
                else:
                    # Write other lines unchanged
                    outfile.write(line + "\n")

        print(f"Cleaned data written to {output_file}")
    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
        sys.exit(1)

def parse_file(file_path):
    """
    Parse the file to build a dictionary mapping sequence IDs to prefixes.
    """
    data = defaultdict(set)
    try:
        with open(file_path, "r") as file:
            for line in file:
                if line.startswith("#") or not line.strip():
                    continue

                columns = line.strip().split()
                if not columns:
                    continue

                first_column = columns[0]
                parts = first_column.split("_")

                if len(parts) < 2:
                    continue

                prefix = parts[0]
                sequence_id = "_".join(parts[1:-1])

                data[sequence_id].add(prefix)

        print(f"Parsed {len(data)} unique sequence IDs.")
        return data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        sys.exit(1)

def write_merged_data(data, output_file):
    """
    Write merged prefix data to the output file.
    """
    with open(output_file, "w") as file:
        for seq_id, prefixes in data.items():
            if ('dnaa' in prefixes and 'core' in prefixes) or \
               ('dnaa' not in prefixes and all(p in prefixes for p in ['rep', 'core', 'par'])):
                file.write(f"{seq_id}\t{','.join(sorted(prefixes))}\n")

    print(f"Merged data written to {output_file}")

def process_files(input_file, output_file):
    """
    Main function to process input file and produce output.
    """
    cleaned_file = "temp_cleaned_file.txt"
    clean_file(input_file, cleaned_file)
    parsed_data = parse_file(cleaned_file)
    write_merged_data(parsed_data, output_file)
    print("Processing complete.")

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: python script.py")
        sys.exit(1)

    input_path = "part1.txt"
    output_path = "part2.txt"

    process_files(input_path, output_path)
