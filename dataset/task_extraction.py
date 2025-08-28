import os
import json
import pandas as pd
import re
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path("./scientific-workflows/WfInstances").resolve()
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
    # Remove trailing numbers and ID suffixes
    cleaned = task_name
    cleaned = re.sub('_\\d+$', '', cleaned)
    cleaned = re.sub('_ID\\d+$', '', cleaned)
    
    if '.' in cleaned:
        cleaned = cleaned.split('.')[-1]
    
    for key in main_task_mapping:
        if key in cleaned.lower():
            return main_task_mapping[key]
    
    return cleaned.lower()

def extract_logical_task_name(task_name):
    if not task_name:
        return task_name
    
    # Remove common physical instance suffixes
    logical_name = task_name
    logical_name = re.sub('_ID\\d+$', '', logical_name)
    logical_name = re.sub('_\\d+$', '', logical_name)
    
    return logical_name

def extract_logical_dependencies(data):
    spec_tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
    
    print(f"Building logical dependency graph...")
    
    # Use the children/parents fields (direct logical dependencies)
    spec_children = defaultdict(set)
    spec_dependencies = defaultdict(set)
    
    # Group tasks by logical name first
    logical_task_groups = defaultdict(list)
    
    for task in spec_tasks:
        task_name = task.get("name")
        if not task_name:
            continue
            
        logical_name = extract_logical_task_name(task_name)
        logical_task_groups[logical_name].append(task)
        
        # Use children field for logical dependencies
        children = task.get("children", [])
        parents = task.get("parents", [])
        
        for child in children:
            if child:
                child_logical = extract_logical_task_name(child)
                spec_children[logical_name].add(child_logical)
        
        for parent in parents:
            if parent:
                parent_logical = extract_logical_task_name(parent)
                spec_dependencies[logical_name].add(parent_logical)
    
    print(f"Found {len(logical_task_groups)} logical task types")
    print(f"Physical instances per logical task:")
    for logical_name, tasks in list(logical_task_groups.items())[:5]:
        print(f"   {logical_name}: {len(tasks)} instances")
    
    # Also build file-based dependencies as backup/validation
    file_children = defaultdict(set)
    file_dependencies = defaultdict(set)
    
    # Build file producers map
    file_producers = {}
    for task in spec_tasks:
        task_name = task.get("name")
        if not task_name:
            continue
            
        logical_name = extract_logical_task_name(task_name)
        
        # Map output files to logical producer
        for out in task.get("outputFiles", []):
            filename = extract_file_names([out])
            for f in filename:
                if f:
                    file_producers[os.path.basename(f)] = logical_name
    
    # Build file-based dependencies between logical tasks
    for task in spec_tasks:
        task_name = task.get("name")
        if not task_name:
            continue
            
        logical_name = extract_logical_task_name(task_name)
        
        # Find dependencies based on input files
        for inp in task.get("inputFiles", []):
            filename = extract_file_names([inp])
            for f in filename:
                if f:
                    producer = file_producers.get(os.path.basename(f))
                    if producer and producer != logical_name:
                        file_dependencies[logical_name].add(producer)
                        file_children[producer].add(logical_name)
    
    print(f"Spec-based: {len(spec_children)} logical tasks with children")
    print(f"File-based: {len(file_children)} logical tasks with children")
    
    # Use spec-based if available, fall back to file-based
    final_children = spec_children if spec_children else file_children
    final_dependencies = spec_dependencies if spec_dependencies else file_dependencies
    
    return final_children, final_dependencies, logical_task_groups

def get_file_size(file_path, size_lookup):
    if not file_path or file_path.lower() == 'none':
        return 0
    
    basename = os.path.basename(file_path)
    normalized_path = os.path.normpath(file_path).replace("\\", "/")
    
    # Try different lookup strategies
    size = (
        size_lookup.get(file_path) or
        size_lookup.get(basename) or
        size_lookup.get(normalized_path) or
        size_lookup.get(file_path.strip()) or
        size_lookup.get(basename.strip())
    )
    
    return size if size is not None else 0

def build_file_size_lookup(data):
    file_size_lookup = {}
    
    print(f"Building file size lookup...")
    
    # 1. Extract from workflow specification files
    files = data.get("workflow", {}).get("specification", {}).get("files", [])
    spec_count = 0
    for f in files:
        if isinstance(f, dict):
            key = f.get("id") or f.get("name") or f.get("filename")
            size = f.get("sizeInBytes") or f.get("size") or f.get("fileSize")
            if key and size is not None:
                try:
                    size = int(float(size))
                    file_size_lookup[key] = size
                    file_size_lookup[os.path.basename(key)] = size
                    spec_count += 1
                except (ValueError, TypeError):
                    continue
    
    # 2. Extract from execution task outputs
    exec_count = 0
    execution_tasks = data.get("workflow", {}).get("execution", {}).get("tasks", [])
    for task in execution_tasks:
        for output in task.get("outputs", []):
            if isinstance(output, dict):
                key = output.get("id") or output.get("name") or output.get("filename")
                size = output.get("sizeInBytes") or output.get("size") or output.get("fileSize")
                if key and size is not None:
                    try:
                        size = int(float(size))
                        file_size_lookup[key] = size
                        file_size_lookup[os.path.basename(key)] = size
                        exec_count += 1
                    except (ValueError, TypeError):
                        continue
    
    # 3. Extract from specification tasks
    spec_task_count = 0
    spec_tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
    for task in spec_tasks:
        # Check input files with sizes
        for inp in task.get("inputFiles", []):
            if isinstance(inp, dict):
                key = inp.get("name") or inp.get("id") or inp.get("filename")
                size = inp.get("size") or inp.get("sizeInBytes") or inp.get("fileSize")
                if key and size is not None:
                    try:
                        size = int(float(size))
                        file_size_lookup[key] = size
                        file_size_lookup[os.path.basename(key)] = size
                        spec_task_count += 1
                    except (ValueError, TypeError):
                        continue
        
        # Check output files with sizes
        for out in task.get("outputFiles", []):
            if isinstance(out, dict):
                key = out.get("name") or out.get("id") or out.get("filename")
                size = out.get("size") or out.get("sizeInBytes") or out.get("fileSize")
                if key and size is not None:
                    try:
                        size = int(float(size))
                        file_size_lookup[key] = size
                        file_size_lookup[os.path.basename(key)] = size
                        spec_task_count += 1
                    except (ValueError, TypeError):
                        continue
    
    # 4. Extract from Pegasus-style command arguments
    pegasus_count = 0
    for task in execution_tasks:
        args = task.get("command", {}).get("arguments", [])
        for arg in args:
            if isinstance(arg, str) and arg.startswith("--out"):
                try:
                    json_str = arg.replace("--out ", "").replace('\\"', '"')
                    file_sizes = json.loads(json_str)
                    for fname, size in file_sizes.items():
                        if size is not None:
                            size = int(float(size))
                            file_size_lookup[fname] = size
                            file_size_lookup[os.path.basename(fname)] = size
                            pegasus_count += 1
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
    
    # 5. Look for file sizes in other common locations
    misc_count = 0
    if "files" in data:
        for file_info in data["files"]:
            if isinstance(file_info, dict):
                key = file_info.get("name") or file_info.get("id") or file_info.get("filename")
                size = file_info.get("size") or file_info.get("sizeInBytes") or file_info.get("fileSize")
                if key and size is not None:
                    try:
                        size = int(float(size))
                        file_size_lookup[key] = size
                        file_size_lookup[os.path.basename(key)] = size
                        misc_count += 1
                    except (ValueError, TypeError):
                        continue
    
    print(f"File size sources: spec_files={spec_count}, exec_outputs={exec_count}, spec_tasks={spec_task_count}, pegasus_args={pegasus_count}, misc={misc_count}")
    print(f"Total unique file sizes found: {len(file_size_lookup)}")
    
    if len(file_size_lookup) > 0:
        print(f"Sample file sizes: {dict(list(file_size_lookup.items())[:5])}")
    
    return file_size_lookup

def extract_file_names(file_list):
    files = []
    if not file_list:
        return files
    
    for item in file_list:
        if isinstance(item, str):
            if item and item.lower() != 'none':
                files.append(item)
        elif isinstance(item, dict):
            name = item.get("name") or item.get("id") or item.get("filename") or item.get("file")
            if name and name.lower() != 'none':
                files.append(name)
    
    return files

def calculate_total_file_sizes(file_list, size_lookup, file_type=""):
    files = extract_file_names(file_list)
    
    total_size = 0
    found_sizes = 0
    missing_files = []
    
    seen_files = set()
    for file_path in files:
        basename = os.path.basename(file_path)
        if basename not in seen_files:
            seen_files.add(basename)
            size = get_file_size(file_path, size_lookup)
            if size > 0:
                total_size += size
                found_sizes += 1
            else:
                missing_files.append(basename)
    
    if missing_files and len(missing_files) <= 3:
        print(f"Missing {file_type} file sizes for: {missing_files[:3]}")
    elif len(missing_files) > 3:
        print(f"Missing {file_type} file sizes for {len(missing_files)} files")
    
    return total_size, len(files), found_sizes

def extract_task_info(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)

    workflow_name = data.get("name", os.path.basename(filepath))
    tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
    file_sizes = build_file_size_lookup(data)
    
    # Extract logical dependencies
    logical_children, logical_dependencies, logical_task_groups = extract_logical_dependencies(data)

    rows = []
    for task in tasks:
        task_name = task.get("name")
        logical_task_name = extract_logical_task_name(task_name)
        task_category = clean_and_map_task_type(task_name)
        
        # Physical children from specification
        physical_children = task.get("children", [])
        if not isinstance(physical_children, list):
            print(f"Task {task_name} has malformed children: {physical_children}")
            physical_children = []

        # Logical children count based on logical task name
        logical_children_count = len(logical_children.get(logical_task_name, set()))
        logical_dependencies_count = len(logical_dependencies.get(logical_task_name, set()))
        
        # Count instances of this logical task
        instances_of_logical_task = len(logical_task_groups.get(logical_task_name, []))

        input_files = task.get("inputFiles", [])
        output_files = task.get("outputFiles", [])

        # Calculate file sizes
        input_size, input_count, input_found = calculate_total_file_sizes(
            input_files, file_sizes, f"INPUT({task_name})"
        )
        output_size, output_count, output_found = calculate_total_file_sizes(
            output_files, file_sizes, f"OUTPUT({task_name})"
        )

        # Debug information for specific tasks
        if 'fastq_reduce' in task_name.lower() or 'bwa_index' in task_name.lower():
            print(f"Task {task_name} (logical: {logical_task_name}):")
            print(f"   Logical children: {logical_children_count}")
            print(f"   Logical dependencies: {logical_dependencies_count}")
            print(f"   Physical children: {len(physical_children)}")
            print(f"   Instances of logical task: {instances_of_logical_task}")

        row = {
            "workflow_name": workflow_name,
            "task_name": task_name,
            "logical_task_name": logical_task_name,
            "task_category": task_category,
            "instance_count": 1,
            "logical_children_count": logical_children_count,
            "logical_dependencies_count": logical_dependencies_count,
            "physical_children_count": len(physical_children),
            "logical_task_instances": instances_of_logical_task,
            "input_file_count": input_count,
            "total_input_file_sizes": input_size,
            "output_file_count": output_count,
            "total_output_file_sizes": output_size,
            "input_files": extract_file_names(input_files),
            "output_files": extract_file_names(output_files),
            "input_sizes_found": input_found,
            "output_sizes_found": output_found
        }

        rows.append(row)

    return rows

def analyze_spec_vs_execution(data):
    spec_tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
    exec_tasks = data.get("workflow", {}).get("execution", {}).get("tasks", [])
    
    print(f"\nSPECIFICATION vs EXECUTION ANALYSIS:")
    print(f"Specification tasks: {len(spec_tasks)}")
    print(f"Execution tasks: {len(exec_tasks)}")
    
    exec_by_logical = defaultdict(list)
    for exec_task in exec_tasks:
        logical_name = (exec_task.get("logicalTaskName") or 
                       exec_task.get("taskName") or 
                       exec_task.get("name") or
                       exec_task.get("type")) 

        if logical_name:
            exec_by_logical[logical_name].append(exec_task)
    
    print(f"\nLOGICAL TASK SPAWNING ANALYSIS:")
    for spec_task in spec_tasks:
        spec_name = spec_task.get("name")
        spec_children = len(spec_task.get("children", []))
        
        exec_instances = exec_by_logical.get(spec_name, [])
        
        print(f"Task '{spec_name}':")
        print(f"  - Spec children: {spec_children}")
        print(f"  - Execution instances: {len(exec_instances)}")
        
        if len(exec_instances) != spec_children and spec_children > 0:
            print(f"  MISMATCH: Spec suggests {spec_children} children, but {len(exec_instances)} were executed")
    
    return exec_by_logical

# MAIN EXECUTION
all_rows = []
seen_workflows = set()

for root, _, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".json"):
            path = os.path.join(root, file)
            try:
                print(f"\nProcessing: {path}")
                rows = extract_task_info(path)
                if rows:
                    wf_name = rows[0]["workflow_name"]
                    if wf_name not in seen_workflows:
                        all_rows.extend(rows)
                        seen_workflows.add(wf_name)
                        print(f"Included workflow: {wf_name} ({len(rows)} tasks)")
                    else:
                        print(f"Skipped duplicate workflow: {wf_name}")
            except Exception as e:
                print(f"Error processing {path}: {e}")

# Task-level DataFrame
task_level_df = pd.DataFrame(all_rows)

if task_level_df.empty:
    print("No tasks found — check input data or paths.")
else:
    # Add summary statistics
    total_tasks = len(task_level_df)
    tasks_with_input_sizes = (task_level_df['input_sizes_found'] > 0).sum()
    tasks_with_output_sizes = (task_level_df['output_sizes_found'] > 0).sum()
    
    
    # Save simplified version without debug columns
    simplified_df = task_level_df.drop(columns=["input_files", "output_files", "input_sizes_found", "output_sizes_found"], errors='ignore')

    # Aggregation with enhanced statistics using logical children
    grouped = []
    for (workflow, logical_task), group in task_level_df.groupby(['workflow_name', 'logical_task_name']):
        instance_count = len(group)
        logical_children_total = group['logical_children_count'].iloc[0]
        logical_dependencies_total = group['logical_dependencies_count'].iloc[0]
        
        task_category = group['task_category'].iloc[0]

        row = {
            "workflow_name": workflow,
            "logical_task_name": logical_task,
            "task_category": task_category,
            "instance_count": instance_count,
            "logical_children_count": logical_children_total,
            "logical_dependencies_count": logical_dependencies_total,
            "input_file_count": group['input_file_count'].sum(),
            "total_input_file_sizes": group['total_input_file_sizes'].sum(),
            "output_file_count": group['output_file_count'].sum(),
            "total_output_file_sizes": group['total_output_file_sizes'].sum(),
            "input_coverage": group['input_sizes_found'].sum() / max(group['input_file_count'].sum(), 1),
            "output_coverage": group['output_sizes_found'].sum() / max(group['output_file_count'].sum(), 1)
        }
        grouped.append(row)

    logical_tasks = pd.DataFrame(grouped)

PROJECT_ROOT = BASE_DIR.parent
RESULTS_DIR  = PROJECT_ROOT / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

detailed_path   = RESULTS_DIR / "task_level_dataset_detailed_test.csv"
simplified_path = RESULTS_DIR / "task_level_dataset_test.csv"
logical_path    = RESULTS_DIR / "logical_task_dataset_test.csv"

# save + prints
task_level_df.to_csv(detailed_path, index=False)
simplified_df.to_csv(simplified_path, index=False)
logical_tasks.to_csv(logical_path, index=False)

print(f"\nSaved detailed task-level dataset with {len(task_level_df)} rows to {detailed_path}")
print(f"Saved simplified task-level dataset to {simplified_path}")
print(f"Saved logical task aggregation with {len(logical_tasks)} rows to {logical_path}")