import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import ast


def read_tetranucleotide_frequencies(temp_file):
    """从 temp_file 加载四核苷酸频率数据"""
    tetranucleotide_frequencies = {}
    with open(temp_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                parts = line.split('\t', 1)
                if len(parts) > 1:
                    try:
                        tetranucleotide_frequencies[parts[0]] = ast.literal_eval(parts[1])
                    except (ValueError, SyntaxError) as e:
                        print(f"[ERROR] Failed to parse frequencies for {parts[0]}: {e}")
    return tetranucleotide_frequencies


def load_prescreen_data(prescreen_file):
    """从 prescreen_file 加载预筛选数据"""
    prescreen_prefixes = {}
    dnaa_sequences = set()

    with open(prescreen_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                seq_id, prefix = line.split('\t')
                prescreen_prefixes[seq_id] = prefix
                if 'dnaa' in prefix:
                    dnaa_sequences.add(seq_id)
    
    print(f"[Note] Loaded prescreen data for {len(prescreen_prefixes)} sequences")
    return prescreen_prefixes, dnaa_sequences


def calculate_relative_abundance_distance(freq1, freq2):
    """计算两个四核苷酸频率之间的距离"""
    return sum((freq1.get(tetranucleotide, 0) - freq2.get(tetranucleotide, 0)) ** 2 for tetranucleotide in freq1)


def process_single_cluster(cluster, sequences, precomputed_frequencies, prescreen_prefixes, distance_threshold):
    """处理单个聚类"""
    dnaa_seq_id = cluster['dnaa'][0]
    if len(cluster['dnaa']) != 1 or dnaa_seq_id not in sequences:
        return None

    dnaa_freq = precomputed_frequencies.get(dnaa_seq_id, {})
    filtered_sequences = [dnaa_seq_id]

    for seq_id in cluster['sequences']:
        if seq_id not in sequences or 'dnaa' in prescreen_prefixes.get(seq_id, ''):
            continue

        seq_freq = precomputed_frequencies.get(seq_id, {})
        distance = calculate_relative_abundance_distance(dnaa_freq, seq_freq)

        if distance <= distance_threshold:
            filtered_sequences.append(seq_id)

    if len(filtered_sequences) > 1:
        return filtered_sequences



def process_clusters_in_chunks(cluster_chunk, sequences_list, tetranucleotide_frequencies, prescreen_prefixes, distance_threshold):
    """处理聚类的多个块"""
    return [cluster for cluster in (process_single_cluster(cluster, sequences_list, tetranucleotide_frequencies, prescreen_prefixes, distance_threshold) for cluster in cluster_chunk) if cluster]


def read_clusters(clustered_output_file):
    """读取聚类数据"""
    clusters, current_cluster = [], None

    with open(clustered_output_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith("Central sequence in the cluster:"):
                if current_cluster:
                    clusters.append(current_cluster)
                current_cluster = {'dnaa': [line.split(":")[1].strip()], 'sequences': []}
            elif line.startswith("Other sequences in the cluster:"):
                if current_cluster:
                    sequences = line.split(":")[1].strip()
                    if sequences:
                        current_cluster['sequences'] = sequences.split(", ")
            elif line == "------":
                continue

        if current_cluster:
            clusters.append(current_cluster)

    print(f"[Note] Loaded {len(clusters)} clusters.")
    return clusters


def filter_sequences(clustered_output_file, prescreen_file, temp_file, final_output_file, cpu, distance_threshold):
    """主过滤函数，处理整个流程"""

    tetranucleotide_frequencies = read_tetranucleotide_frequencies(temp_file)
    prescreen_prefixes, _ = load_prescreen_data(prescreen_file)
    clusters = read_clusters(clustered_output_file)

    sequences_list = list(tetranucleotide_frequencies.keys())
    chunk_size = max(1, len(clusters) // cpu)
    cluster_chunks = list(grouper(clusters, chunk_size))

    with ProcessPoolExecutor(max_workers=cpu) as executor:
        futures = [executor.submit(process_clusters_in_chunks, chunk, sequences_list, tetranucleotide_frequencies, prescreen_prefixes, distance_threshold) for chunk in cluster_chunks]

        filtered_clusters = []
        for future in as_completed(futures):
            try:
                filtered_clusters.extend(future.result())
            except Exception as e:
                print(f"Error processing cluster chunk: {e}")

    print(f"[Note] Finally clustered {len(filtered_clusters)} clusters.")

    with open(final_output_file, "w") as outfile:
        for cluster in filtered_clusters:
            outfile.write("Possible bacterial chromosome:\n")
            outfile.write(f"{cluster[0]}\n")
            outfile.write("Possible bacterial chromids:\n")
            outfile.write(", ".join(cluster[1:]) + "\n")
            outfile.write("------\n")


def grouper(iterable, n):
    """将列表分割成固定大小的块"""
    args = [iter(iterable)] * n
    return zip(*args)


# 主函数入口
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <final_output_file> <cpu> <dt>")
        sys.exit(1)

    clustered_output_file = "part4.txt"
    prescreen_file = "part2.txt"
    temp_file = "part3.txt"
    final_output_file = sys.argv[1]
    cpu = int(sys.argv[2])
    distance_threshold = float(sys.argv[3])
    
    filter_sequences(clustered_output_file, prescreen_file, temp_file, final_output_file, cpu , distance_threshold)
