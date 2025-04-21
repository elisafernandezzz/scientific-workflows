import os
import json
import pandas as pd
import re
from collections import defaultdict

BASE_DIR = "./WfInstances"
print(f"Scanning in: {BASE_DIR}")

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
        cleaned = cleaned.split('.')[-1]  # More precise subtask
    
    # Match known keywords
    for key in main_task_mapping:
        if key in cleaned.lower():
            return main_task_mapping[key]
    
    return cleaned.lower()


def get_file_size(file_path, size_lookup):
    basename = os.path.basename(file_path)
    return (
        size_lookup.get(file_path)
        or size_lookup.get(basename)
        or size_lookup.get(os.path.normpath(file_path).replace("\\", "/"))
        or 0
    )

def build_file_size_lookup(data):
    file_size_lookup = {}

    files = data.get("workflow", {}).get("specification", {}).get("files", [])
    for f in files:
        if isinstance(f, dict):
            key = f.get("id") or f.get("name")
            size = f.get("sizeInBytes") or f.get("size")
            if key and size:
                file_size_lookup[os.path.basename(key)] = size
                file_size_lookup[key] = size

    # Also look inside command arguments for Pegasus-style `--out` size info
    tasks = data.get("workflow", {}).get("execution", {}).get("tasks", [])
    for task in tasks:
        args = task.get("command", {}).get("arguments", [])
        for arg in args:
            if isinstance(arg, str) and arg.startswith("--out"):
                try:
                    json_str = arg.replace("--out ", "").replace('\\"', '"')
                    file_sizes = json.loads(json_str)
                    for fname, size in file_sizes.items():
                        file_size_lookup[os.path.basename(fname)] = size
                        file_size_lookup[fname] = size
                except json.JSONDecodeError:
                    continue

    return file_size_lookup

def extract_task_info(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)

    workflow_name = data.get("name", os.path.basename(filepath))
    tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
    file_sizes = build_file_size_lookup(data)

    rows = []
    for task in tasks:
        task_name = task.get("name")
        task_category = clean_and_map_task_type(task_name)
        children = task.get("children", [])
        if not isinstance(children, list):
            print(f"⚠ Task {task_name} has malformed children: {children}")
            children = []

        input_files = task.get("inputFiles", [])
        output_files = task.get("outputFiles", [])

        seen_input = set()
        input_size = sum(
            get_file_size(f, file_sizes)
            for f in input_files
            if f and f.lower() != "none" and os.path.basename(f) not in seen_input and not seen_input.add(os.path.basename(f))
        )

        seen_output = set()
        output_size = sum(
            get_file_size(f, file_sizes)
            for f in output_files
            if f and f.lower() != "none" and os.path.basename(f) not in seen_output and not seen_output.add(os.path.basename(f))
        )

        row = {
            "workflow_name": workflow_name,
            "task_name": task_name,
            "task_category": task_category,
            "instance_count": 1,
            "children_count": len(children),
            "input_file_count": len(input_files),
            "total_input_file_sizes": input_size,
            "output_file_count": len(output_files),
            "total_output_file_sizes": output_size,
            "input_files": input_files,
            "output_files": output_files
        }

        rows.append(row)

    return rows

# MAIN EXECUTION
all_rows = []
seen_workflows = set()

for root, _, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".json"):
            path = os.path.join(root, file)
            try:
                rows = extract_task_info(path)
                if rows:
                    wf_name = rows[0]["workflow_name"]
                    if wf_name not in seen_workflows:
                        all_rows.extend(rows)
                        seen_workflows.add(wf_name)
                        print(f"✔ Included workflow: {wf_name}")
                    else:
                        print(f"⏩ Skipped duplicate workflow: {wf_name}")
            except Exception as e:
                print(f"✖ Error processing {path}: {e}")

# Task-level DataFrame
task_level_df = pd.DataFrame(all_rows)

if task_level_df.empty:
    print("⚠ No tasks found — check input data or paths.")
else:
    task_level_df.to_csv("task_level_dataset_detailed.csv", index=False)
    simplified_df = task_level_df.drop(columns=["input_files", "output_files"], errors='ignore')
    simplified_df.to_csv("task_level_dataset.csv", index=False)

    # Aggregation
    grouped = []
    for (workflow, category), group in task_level_df.groupby(['workflow_name', 'task_category']):
        instance_count = len(group)
        row = {
            "workflow_name": workflow,
            "task_category": category,
            "task_name": group['task_name'].iloc[0],
            "instance_count": instance_count,
            "children_count": group['children_count'].sum(),
            "input_file_count": group['input_file_count'].sum(),
            "total_input_file_sizes": group['total_input_file_sizes'].sum(),
            "output_file_count": group['output_file_count'].sum(),
            "total_output_file_sizes": group['total_output_file_sizes'].sum()
        }
        grouped.append(row)

    logical_tasks = pd.DataFrame(grouped)
    logical_tasks.to_csv("logical_task_dataset.csv", index=False)

    print("\n--- LOGICAL TASK SUMMARY (CSV-style) ---")
    for _, row in logical_tasks.iterrows():
        print(f"{row['workflow_name']},{row['task_category']},{row['task_name']},"
              f"{row['instance_count']},{int(row['children_count'])},"
              f"{row['input_file_count']},{int(row['total_input_file_sizes'])},"
              f"{row['output_file_count']},{int(row['total_output_file_sizes'])}")

    print(f"✅ Saved task-level dataset with {len(task_level_df)} tasks to task_level_dataset.csv")
    print(f"✅ Saved detailed task-level dataset to task_level_dataset_detailed.csv")
    print(f"✅ Saved logical task aggregation with {len(logical_tasks)} entries to logical_task_dataset.csv")
