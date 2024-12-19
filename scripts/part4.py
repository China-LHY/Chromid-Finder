import sys
import pandas as pd

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

    return dnaa_df, merged_df

def find_clusters(dnaa_row, merged_df, gc_threshold=1):
    """Find clusters for a given dnaa row based on GC and length criteria."""
    cluster = [dnaa_row['sequence_id']]
    for _, row in merged_df.iterrows():
        if row['sequence_id'] == dnaa_row['sequence_id']:
            continue
        if abs(row['GC'] - dnaa_row['GC']) <= gc_threshold and row['length'] < dnaa_row['length']:
            cluster.append(row['sequence_id'])
    return cluster

def cluster_sequences(gc_file, intput_file, part4_file):
    """Main logic to cluster sequences based on GC and length criteria."""
    dnaa_df, merged_df = load_and_preprocess_data(gc_file, intput_file)

    clusters = []
    for _, row in dnaa_df.iterrows():
        clusters.append(find_clusters(row, merged_df))

    # Write clusters to file
    with open(part4_file, "w") as f:
        for cluster in clusters:
            f.write(f"Central sequence in the cluster: {cluster[0]}\n")
            f.write(f"Other sequences in the cluster: {', '.join(cluster[1:])}\n")
            f.write("------\n")
    print(f"Clustered data written to {part4_file}")

def main(gc_file, intput_file, part4_file):
    cluster_sequences(gc_file, intput_file, part4_file)

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: python script.py")
        sys.exit(1)

    gc_file = "gc.tsv"
    intput_file = "part2.txt"
    part4_file = "part4.txt"

    main(gc_file, intput_file, part4_file)
