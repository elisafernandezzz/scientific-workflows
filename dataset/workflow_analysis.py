import os
import json
import pandas as pd
from collections import defaultdict
import re
from pathlib import Path

def extract_workflow_type(workflow_name):
    """Extract workflow type from workflow name"""
    # Common patterns in scientific workflows
    name_lower = workflow_name.lower()
    
    # Direct matches
    if 'scrnaseq' in name_lower:
        return 'scrnaseq'
    elif 'smrnaseq' in name_lower:
        return 'smrnaseq'
    if 'blast' in name_lower:
        return 'blast'
    elif 'montage' in name_lower:
        return 'montage'
    elif 'sipht' in name_lower:
        return 'sipht'
    elif 'cycles' in name_lower:
        return 'cycles'
    elif 'epigenome' in name_lower:
        return 'epigenome'
    elif 'soykb' in name_lower:
        return 'soykb'
    elif 'seismology' in name_lower:
        return 'seismology'
    elif '1000genome' in name_lower:
        return '1000genome'
    elif name_lower.startswith('genome-dax') or 'epigenomics' in name_lower:
        return 'epigenomics'
    elif name_lower == 'genome' or name_lower.startswith('genome-'):
        return 'genome'
    elif 'rnaseq' in name_lower:
        return 'rnaseq'
    elif 'helloworld' in name_lower:
        return 'helloworld'
    elif 'chain' in name_lower:
        return 'chain'
    elif 'forkjoin' in name_lower:
        return 'forkjoin'
    elif 'cpuhog' in name_lower:
        return 'cpuhog'
    elif 'bwa' in name_lower:
        return 'bwa'
    elif 'alignment' in name_lower:
        return 'alignment'
    elif 'epigenomics' in name_lower:
        return 'epigenomics'
    elif 'workflow-test' in name_lower:
        return 'srasearch'
    
    # Try to extract from path or name patterns
    # Remove common suffixes and numbers
    cleaned = re.sub(r'-\d+.*$', '', name_lower)  # Remove -001, -large-001, etc.
    cleaned = re.sub(r'_\d+.*$', '', cleaned)     # Remove _001, etc.
    
    return cleaned

def clean_and_map_task_type(task_name):
    """Enhanced task type mapping"""
    cleaned = re.sub(r'(_\d+)$', '', task_name)
    cleaned = re.sub(r'_ID\d+$', '', cleaned)
    
    if '.' in cleaned:
        cleaned = cleaned.split('.')[-1]
    
    # Enhanced mapping
    main_task_mapping = {
        'fastp': 'preprocessing', 'cutadapt': 'trimming', 'trimmomatic': 'trimming', 
        'awk': 'filtering', 'grep': 'filtering', 'sed': 'filtering',
        'bwa': 'alignment', 'bowtie2': 'alignment', 'star': 'alignment', 'hisat2': 'alignment',
        'kallisto': 'quantification', 'salmon': 'quantification', 'htseq': 'quantification', 
        'featurecounts': 'quantification', 'rsem': 'quantification',
        'picard': 'deduplication', 'umi_tools': 'deduplication',
        'fastqc': 'qc', 'frag_len_hist': 'qc', 'multiqc': 'reporting',
        'tabix': 'indexing', 'index': 'indexing', 'samtools_index': 'indexing',
        'samtools': 'file_handling', 'merge': 'file_handling', 'bedtools': 'genomic_interval_ops',
        'ucsc': 'visualization', 'deeptools': 'visualization',
        'preseq': 'complexity_estimation',
        'blastall': 'blast_search', 'blast': 'blast_search',
        'split_fasta': 'data_splitting', 'split': 'data_splitting',
        'cat': 'file_concatenation', 'concatenate': 'file_concatenation',
        'cpuhog': 'compute_intensive', 'cpu': 'compute_intensive',
        'montage': 'image_processing', 'mproject': 'image_processing', 'mimgtbl': 'image_processing',
        'mhdr': 'image_processing', 'madd': 'image_processing',
        'sipht': 'rna_analysis', 'findterm': 'rna_analysis', 'blast2btab': 'rna_analysis',
        'patser': 'sequence_analysis', 'hmmsearch': 'sequence_analysis'
    }
    
    # Match known keywords
    for key in main_task_mapping:
        if key in cleaned.lower():
            return main_task_mapping[key]
    
    return cleaned.lower()

def create_workflow_task_table(base_dir):
    """Create comprehensive table of workflow types and their logical tasks"""
    
    workflow_tasks = defaultdict(set)  # workflow_type -> set of task_categories
    workflow_details = defaultdict(lambda: {
        'count': 0, 
        'total_tasks': 0, 
        'avg_tasks_per_workflow': 0,
        'task_details': defaultdict(lambda: {'count': 0, 'instances': 0})
    })
    
    print(f"🔍 Scanning workflows in: {base_dir}")
    
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                path = os.path.join(root, file)
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    
                    workflow_name = data.get("name", os.path.basename(file))
                    workflow_type = extract_workflow_type(workflow_name)
                    
                    tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
                    
                    if tasks:
                        workflow_details[workflow_type]['count'] += 1
                        workflow_details[workflow_type]['total_tasks'] += len(tasks)
                        
                        for task in tasks:
                            task_name = task.get("name", "")
                            task_category = clean_and_map_task_type(task_name)
                            
                            workflow_tasks[workflow_type].add(task_category)
                            workflow_details[workflow_type]['task_details'][task_category]['count'] += 1
                            workflow_details[workflow_type]['task_details'][task_category]['instances'] += 1
                
                except Exception as e:
                    print(f"⚠ Error processing {path}: {e}")
    
    # Calculate averages
    for wf_type in workflow_details:
        details = workflow_details[wf_type]
        if details['count'] > 0:
            details['avg_tasks_per_workflow'] = details['total_tasks'] / details['count']
    
    # Create summary table
    summary_data = []
    for workflow_type in sorted(workflow_tasks.keys()):
        task_list = sorted(list(workflow_tasks[workflow_type]))
        details = workflow_details[workflow_type]
        
        summary_data.append({
            'workflow_type': workflow_type,
            'workflow_count': details['count'],
            'total_tasks': details['total_tasks'],
            'avg_tasks_per_workflow': round(details['avg_tasks_per_workflow'], 1),
            'unique_task_types': len(task_list),
            'logical_tasks': ', '.join(task_list)
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values('workflow_count', ascending=False)
    
    # Create detailed task breakdown table
    detailed_data = []
    for workflow_type in sorted(workflow_tasks.keys()):
        details = workflow_details[workflow_type]
        for task_category, task_info in details['task_details'].items():
            detailed_data.append({
                'workflow_type': workflow_type,
                'task_category': task_category,
                'task_instances': task_info['instances'],
                'workflows_with_this_task': task_info['count']
            })
    
    detailed_df = pd.DataFrame(detailed_data)
    detailed_df = detailed_df.sort_values(['workflow_type', 'task_instances'], ascending=[True, False])
    
    return summary_df, detailed_df

# Run the analysis
BASE_DIR = Path("./scientific-workflows/WfInstances").resolve()
summary_table, detailed_table = create_workflow_task_table(BASE_DIR)

# --- where to save ---
PROJECT_ROOT = BASE_DIR.parent                 # .../scientific-workflows
RESULTS_DIR  = PROJECT_ROOT / "results"        # .../scientific-workflows/results
RESULTS_DIR.mkdir(parents=True, exist_ok=True) # create if missing

# Save to results/
summary_path  = RESULTS_DIR / "workflow_types_summary.csv"
detailed_path = RESULTS_DIR / "workflow_task_details.csv"

print("\n📊 WORKFLOW TYPES AND LOGICAL TASKS SUMMARY:")
print("="*80)
print(summary_table.to_string(index=False, max_colwidth=50))

print("\n💾 Saving tables to CSV files...")
summary_table.to_csv(summary_path, index=False)
detailed_table.to_csv(detailed_path, index=False)
print(f"✔ Saved: {summary_path}")
print(f"✔ Saved: {detailed_path}")
