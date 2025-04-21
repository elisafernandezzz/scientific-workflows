import os
import json
import pandas as pd
import re
from collections import defaultdict

BASE_DIR = "./WfInstances"

main_task_mapping = {
    'fastp': 'preprocessing', 'cutadapt': 'trimming', 'trimmomatic': 'trimming', 'awk': 'filtering',
    'bwa': 'alignment', 'bowtie2': 'alignment', 'star': 'alignment', 'hisat2': 'alignment',
    'kallisto': 'quantification', 'salmon': 'quantification', 'htseq': 'quantification', 'featurecounts': 'quantification',
    'picard': 'deduplication', 'umi_tools': 'deduplication',
    'fastqc': 'qc', 'frag_len_hist': 'qc', 'multiqc': 'reporting',
    'tabix': 'indexing', 'index': 'indexing', 'samtools_index': 'indexing',
    'samtools': 'file_handling', 'merge': 'file_handling', 'bedtools': 'genomic_interval_ops',
    'ucsc': 'visualization', 'deeptools': 'visualization',
    'preseq': 'complexity_estimation'
}

def clean_and_map_task_type(task_name):
    cleaned = re.sub(r'(_\d+)$', '', task_name)
    cleaned = re.sub(r'_ID\d+$', '', cleaned)
    if '.' in cleaned:
        parts = cleaned.split('.')
        cleaned = parts[-2] if len(parts) > 1 else parts[-1]
    else:
        cleaned = cleaned.split('_')[0]
    for key in main_task_mapping:
        if key in cleaned.lower():
            return main_task_mapping[key]
    return cleaned.lower()

def build_file_size_lookup(data):
    file_size_lookup = {}
    if isinstance(data, dict) and "workflow" in data:
        workflow = data["workflow"]
        if "specification" in workflow:
            spec = workflow["specification"]
            if "files" in spec:
                for file_entry in spec["files"]:
                    if isinstance(file_entry, dict) and "id" in file_entry and "sizeInBytes" in file_entry:
                        filename = os.path.basename(file_entry["id"])
                        file_size_lookup[filename] = file_entry["sizeInBytes"]
    return file_size_lookup

def extract_task_info(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)

    workflow_name = data.get('workflowName') or data.get('name') or os.path.basename(filepath).split('-')[0]
    if 'workflow' in data:
        workflow_name = data['workflow'].get('name', workflow_name)

    file_size_lookup = build_file_size_lookup(data)

    nodes = []
    if 'dag' in data and 'nodes' in data['dag']:
        nodes = data['dag']['nodes']
    elif 'workflow' in data and 'specification' in data['workflow']:
        spec = data['workflow']['specification']
        if 'tasks' in spec:
            nodes = spec['tasks']

    task_type_counts = defaultdict(int)
    rows = []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        task_name = node.get('type') or node.get('name') or node.get('id') or 'unknown_task'
        task_category = clean_and_map_task_type(task_name)
        task_type_counts[task_category] += 1

    for node in nodes:
        if not isinstance(node, dict):
            continue
        task_name = node.get('type') or node.get('name') or node.get('id') or 'unknown_task'
        task_category = clean_and_map_task_type(task_name)

        input_files = node.get('inputFiles', [])
        output_files = node.get('outputFiles', [])
        children = node.get('children', []) if 'children' in node else []

        def compute_size(files):
            size = 0
            for f in files:
                if isinstance(f, str):
                    fname = os.path.basename(f)
                    size += file_size_lookup.get(fname, 0)
            return size

        input_size = compute_size(input_files)
        output_size = compute_size(output_files)

        row = {
            "workflow_name": workflow_name,
            "task_name": task_name,
            "task_category": task_category,
            "instance_count": task_type_counts[task_category],
            "children_count": len(children),
            "input_file_count": len(input_files),
            "total_input_file_sizes": input_size,
            "output_file_count": len(output_files),
            "total_output_file_sizes": output_size
        }
        rows.append(row)

    return rows

# MAIN SCRIPT
all_rows = []
for root, _, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".json"):
            path = os.path.join(root, file)
            print(f"Processing: {path}")
            try:
                rows = extract_task_info(path)
                print(f"  → Extracted {len(rows)} tasks")
                all_rows.extend(rows)
            except Exception as e:
                print(f"  ✖ Failed to process {path}: {e}")

task_level_df = pd.DataFrame(all_rows)

if task_level_df.empty:
    print("⚠ No tasks found.")
else:
    task_level_df.to_csv("task_level_dataset_prueba3.csv", index=False)

    # ✅ Fixed aggregation using accurate file sizes
    logical_tasks = task_level_df.groupby(['workflow_name', 'task_category']).agg({
        'task_name': 'first',
        'instance_count': 'max',
        'children_count': 'mean',
        'input_file_count': 'sum',
        'total_input_file_sizes': 'sum',
        'output_file_count': 'sum',
        'total_output_file_sizes': 'sum'
    }).reset_index()

    logical_tasks.rename(columns={
        "total_input_file_sizes": "total_input_file_sizes",
        "total_output_file_sizes": "total_output_file_sizes"
    }, inplace=True)

    logical_tasks.to_csv("logical_task_dataset_prueba3.csv", index=False)

    print(f"✅ Saved task-level dataset with {len(task_level_df)} tasks to task_level_dataset.csv")
    print(f"✅ Saved logical task aggregation with {len(logical_tasks)} logical tasks to logical_task_dataset.csv")
