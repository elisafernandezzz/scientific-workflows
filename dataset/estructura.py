import os
import json
from collections import defaultdict

def analyze_json_structure(filepath, max_files=3):
    """Analyze the structure of JSON files to understand execution data location"""
    
    print(f"\n🔍 ANALYZING JSON STRUCTURE: {filepath}")
    
    with open(filepath, "r") as f:
        data = json.load(f)
    
    # Print top-level keys
    print(f"📁 Top-level keys: {list(data.keys())}")
    
    # Analyze workflow structure
    if "workflow" in data:
        workflow = data["workflow"]
        print(f"📁 Workflow keys: {list(workflow.keys())}")
        
        # Check specification
        if "specification" in workflow:
            spec = workflow["specification"]
            print(f"📁 Specification keys: {list(spec.keys())}")
            if "tasks" in spec:
                print(f"📊 Specification tasks count: {len(spec['tasks'])}")
                if spec['tasks']:
                    print(f"📋 Sample spec task keys: {list(spec['tasks'][0].keys())}")
        
        # Check execution - this is the key part!
        if "execution" in workflow:
            exec_data = workflow["execution"]
            print(f"📁 Execution keys: {list(exec_data.keys())}")
            if "tasks" in exec_data:
                print(f"📊 Execution tasks count: {len(exec_data['tasks'])}")
                if exec_data['tasks']:
                    print(f"📋 Sample exec task keys: {list(exec_data['tasks'][0].keys())}")
                    # Show a sample execution task
                    sample_exec = exec_data['tasks'][0]
                    print(f"📋 Sample execution task: {json.dumps(sample_exec, indent=2)[:500]}...")
            else:
                print("⚠ No 'tasks' found in execution data")
        else:
            print("⚠ No 'execution' found in workflow")
    
    # Check for other possible execution data locations
    other_keys = [k for k in data.keys() if k not in ['workflow', 'name', 'description']]
    if other_keys:
        print(f"📁 Other top-level keys that might contain execution data: {other_keys}")
        for key in other_keys[:3]:  # Check first 3 other keys
            if isinstance(data[key], (dict, list)):
                print(f"📁 {key} structure: {type(data[key])}")
                if isinstance(data[key], dict):
                    print(f"📁 {key} keys: {list(data[key].keys())}")
                elif isinstance(data[key], list) and data[key]:
                    print(f"📁 {key} list length: {len(data[key])}")
                    if isinstance(data[key][0], dict):
                        print(f"📁 {key}[0] keys: {list(data[key][0].keys())}")

def find_execution_patterns(base_dir, max_files=5):
    """Analyze multiple files to find patterns in execution data"""
    
    print(f"🔍 SEARCHING FOR EXECUTION DATA PATTERNS in {base_dir}")
    
    files_analyzed = 0
    execution_patterns = defaultdict(int)
    
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json") and files_analyzed < max_files:
                path = os.path.join(root, file)
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    
                    # Track different patterns where execution data might be
                    patterns = []
                    
                    # Pattern 1: workflow.execution.tasks
                    exec_tasks = data.get("workflow", {}).get("execution", {}).get("tasks", [])
                    if exec_tasks:
                        patterns.append(f"workflow.execution.tasks ({len(exec_tasks)} tasks)")
                    
                    # Pattern 2: workflow.jobs or workflow.job
                    jobs = data.get("workflow", {}).get("jobs", []) or data.get("workflow", {}).get("job", [])
                    if jobs:
                        patterns.append(f"workflow.jobs ({len(jobs)} jobs)")
                    
                    # Pattern 3: Direct execution key
                    if "execution" in data:
                        exec_data = data["execution"]
                        if isinstance(exec_data, dict) and "tasks" in exec_data:
                            patterns.append(f"execution.tasks ({len(exec_data['tasks'])} tasks)")
                        elif isinstance(exec_data, list):
                            patterns.append(f"execution list ({len(exec_data)} items)")
                    
                    # Pattern 4: Jobs at top level
                    if "jobs" in data:
                        patterns.append(f"jobs ({len(data['jobs'])} jobs)")
                    
                    # Pattern 5: Traces or runs
                    for key in ["traces", "runs", "instances", "executions"]:
                        if key in data:
                            patterns.append(f"{key} ({len(data[key]) if isinstance(data[key], list) else 'dict'})")
                    
                    if not patterns:
                        patterns.append("NO_EXECUTION_DATA")
                    
                    for pattern in patterns:
                        execution_patterns[pattern] += 1
                    
                    if files_analyzed < 3:  # Detailed analysis for first 3 files
                        analyze_json_structure(path)
                    
                    files_analyzed += 1
                    
                except Exception as e:
                    print(f"Error analyzing {path}: {e}")
    
    print(f"\n📊 EXECUTION DATA PATTERNS FOUND:")
    for pattern, count in sorted(execution_patterns.items(), key=lambda x: x[1], reverse=True):
        print(f"  {pattern}: {count} files")

# Run the analysis
BASE_DIR = "./scientific-workflows/WfInstances"
find_execution_patterns(BASE_DIR, max_files=10)