import os
import json
import pandas as pd
import re

# Change this to the location where you cloned the repo
BASE_DIR = "./WfInstances"

# Prepare list to hold dataset rows
data = []

# List of known workflow systems and workflow names associated with them
known_workflow_systems = {
    "nextflow": ["mag", "airrflow", "rnaseq", "atacseq", "chipseq", "cutandrun", "fetchngs", "hic", "methylseq", "sarek", "scrnaseq", "smrnaseq", "taxprofiler", "viralrecon", "bacass"],
    "makeflow": ["makeflow-blast-large", "makeflow-blast-medium", "makeflow-blast-small", "makeflow-bwa-large", "makeflow-bwa-medium", "makeflow-bwa-small"],
    "helloworld": ["chain", "forkjoin"],
}

main_task_mapping = {
    # Preprocessing / Filtering
    'fastp': 'preprocessing',
    'cutadapt': 'trimming',
    'trimmomatic': 'trimming',
    'awk': 'filtering',

    # Alignment
    'bwa': 'alignment',
    'bowtie2': 'alignment',
    'star': 'alignment',
    'hisat2': 'alignment',

    # Quantification
    'kallisto': 'quantification',
    'salmon': 'quantification',
    'htseq': 'quantification',
    'featurecounts': 'quantification',

    # Deduplication
    'picard': 'deduplication',
    'umi_tools': 'deduplication',

    # QC and Reporting
    'fastqc': 'qc',
    'frag_len_hist': 'qc',
    'multiqc': 'reporting',

    # Indexing
    'tabix': 'indexing',
    'index': 'indexing',
    'samtools_index': 'indexing',

    # File Handling / Postprocessing
    'samtools': 'file_handling',
    'merge': 'file_handling',
    'bedtools': 'genomic_interval_ops',

    # Visualization
    'ucsc': 'visualization',
    'deeptools': 'visualization',

    # Others
    'preseq': 'complexity_estimation',
}


def clean_and_map_task_type(task_name, workflow_system):
    cleaned = re.sub(r'(_\d+)$', '', task_name)
    cleaned = re.sub(r'_ID\d+$', '', cleaned)

    if workflow_system == 'nextflow' and '.' in cleaned:
        cleaned_parts = cleaned.split('.')
        # Use second-to-last or descriptive part if available
        if len(cleaned_parts) > 1:
            cleaned = cleaned_parts[-2].lower()
        else:
            cleaned = cleaned_parts[-1].lower()

    if workflow_system != 'nextflow' and '_' in cleaned:
        cleaned = cleaned.split('_')[0].lower()

    # Map to main category if keyword matches
    for key in main_task_mapping:
        if key in cleaned:
            return main_task_mapping[key]

    return cleaned

def parse_json_file(filepath, workflow_system):
    with open(filepath, 'r') as f:
        wf_data = json.load(f)

    workflow_name = wf_data.get('workflowName') or wf_data.get('name') or os.path.basename(filepath).split('-')[0]

    num_tasks = 0
    task_types_count = {}

    if 'dag' in wf_data and 'nodes' in wf_data['dag']:
        nodes = wf_data['dag']['nodes']
        num_tasks = len(nodes)
        for node in nodes:
            raw_type = node.get('type') or node.get('name') or 'unknown_task_type'
            task_type = clean_and_map_task_type(raw_type, workflow_system)
            task_types_count[task_type] = task_types_count.get(task_type, 0) + 1
    elif 'workflow' in wf_data and 'specification' in wf_data['workflow'] and 'tasks' in wf_data['workflow']['specification']:
        tasks = wf_data['workflow']['specification']['tasks']
        num_tasks = len(tasks)
        for task in tasks:
            raw_type = task.get('type') or task.get('name') or 'unknown_task_type'
            task_type = clean_and_map_task_type(raw_type, workflow_system)
            task_types_count[task_type] = task_types_count.get(task_type, 0) + 1

    return workflow_name, num_tasks, task_types_count

rows = []

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(root, file)

            parts = root.split(os.sep)
            workflow_system = next((p for p in parts if p in ["nextflow", "pegasus", "swift", "makeflow", "helloworld"]), "unknown")

            workflow_name, num_tasks, task_types_count = parse_json_file(file_path, workflow_system)

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
                "task_types": "; ".join(f"{k}: {v}" for k, v in task_types_count.items()) if task_types_count else "none"
            }

            rows.append(row)

# Convert to DataFrame and sort
df = pd.DataFrame(rows)
df = df.sort_values(by=["workflow_system", "workflow_name", "instance_file"])

# Save to CSV
df.to_csv("workflow_instances_refined_nextflow_task_mapping.csv", index=False)

print(f"Dataset saved with {len(df)} rows and improved Nextflow task name mapping!")
