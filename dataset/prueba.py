import os
import json
import pandas as pd
import re

BASE_DIR = "./WfInstances"

data = []

known_workflow_systems = {
    "nextflow": ["mag", "airrflow", "rnaseq", "atacseq", "chipseq", "cutandrun", "fetchngs", "hic", "methylseq", "sarek", "scrnaseq", "smrnaseq", "taxprofiler", "viralrecon", "bacass"],
    "makeflow": ["makeflow-blast-large", "makeflow-blast-medium", "makeflow-blast-small", "makeflow-bwa-large", "makeflow-bwa-medium", "makeflow-bwa-small"],
    "helloworld": ["chain", "forkjoin"],
}

task_group_map = {
    'preprocessing': ['fastp', 'cutadapt', 'filter', 'split', 'baseline', 'sifting', 'select'],
    'alignment': ['bwa', 'bowtie2', 'blastall', 'align'],
    'qc': ['fastqc', 'frag_len_hist', 'samtools'],
    'postprocessing': ['cat', 'merge', 'sort', 'combine', 'add', 'bedtools'],
    'indexing': ['index', 'tabix'],
    'visualization': ['deeptools', 'ucsc'],
    'reporting': ['multiqc'],
    'deduplication': ['picard'],
    'complexity_estimation': ['preseq'],
}

# Flatten for matching
flat_task_group_map = {}
for group, keywords in task_group_map.items():
    for keyword in keywords:
        flat_task_group_map[keyword] = group

def clean_and_map_task_type(task_name, workflow_system):
    cleaned = re.sub(r'(_\d+)$', '', task_name)
    cleaned = re.sub(r'_ID\d+$', '', cleaned)

    if workflow_system == 'nextflow' and '.' in cleaned:
        parts = cleaned.split('.')
        cleaned = parts[-2].lower() if len(parts) > 1 else parts[-1].lower()
    elif workflow_system != 'nextflow' and '_' in cleaned:
        cleaned = cleaned.split('_')[0].lower()

    cleaned = cleaned.lower()
    for keyword, group in flat_task_group_map.items():
        if keyword in cleaned:
            return group

    return 'other'

def parse_json_file(filepath, workflow_system):
    with open(filepath, 'r') as f:
        wf_data = json.load(f)

    workflow_name = wf_data.get('workflowName') or wf_data.get('name') or os.path.basename(filepath).split('-')[0]

    num_tasks = 0
    group_counts = {}
    raw_task_list = []

    task_lists = []
    if 'dag' in wf_data and 'nodes' in wf_data['dag']:
        task_lists.append(wf_data['dag']['nodes'])
    elif 'workflow' in wf_data and 'specification' in wf_data['workflow'] and 'tasks' in wf_data['workflow']['specification']:
        task_lists.append(wf_data['workflow']['specification']['tasks'])

    for task_list in task_lists:
        num_tasks += len(task_list)
        for node in task_list:
            raw_type = node.get('type') or node.get('name') or 'unknown_task_type'
            group = clean_and_map_task_type(raw_type, workflow_system)
            group_counts[group] = group_counts.get(group, 0) + 1
            raw_task_list.append(raw_type)

    return workflow_name, num_tasks, group_counts, raw_task_list

rows = []

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(root, file)

            parts = root.split(os.sep)
            workflow_system = next((p for p in parts if p in ["nextflow", "pegasus", "swift", "makeflow", "helloworld"]), "unknown")

            workflow_name, num_tasks, group_counts, raw_task_list = parse_json_file(file_path, workflow_system)

            if workflow_system == "unknown":
                for system, wf_names in known_workflow_systems.items():
                    if any(wf_name.lower() in workflow_name.lower() for wf_name in wf_names):
                        workflow_system = system
                        break

            row = {
                "workflow_system": workflow_system,
                "workflow_name": workflow_name,
                "instance_file": file,
                "num_tasks": num_tasks,
                "task_groups": "; ".join(f"{k}: {v}" for k, v in group_counts.items()) if group_counts else "none"
            }

            for group in task_group_map.keys():
                row[f"group_{group}"] = group_counts.get(group, 0)
            row["group_other"] = group_counts.get("other", 0)

            rows.append(row)

# Convert to DataFrame
df = pd.DataFrame(rows)
df = df.sort_values(by=["workflow_system", "workflow_name", "instance_file"])

df.to_csv("workflow_instances_grouped_task_mapping.csv", index=False)

print(f"Dataset saved with {len(df)} rows and grouped task mappings!")
