import json
import pandas as pd

# Load the JSON file
with open("./scientific-workflows/WfInstances/pegasus/epigenomics/epigenomics-chameleon-hep-1seq-50k-001.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Navigate to the correct execution block
execution = data.get("workflow", {}).get("execution", {})
tasks = execution.get("tasks", [])

if not tasks:
    print("No tasks found under 'workflow -> execution'.")
else:
    # Collect task details into a list of dictionaries
    task_data = []
    for task in tasks:
        task_data.append({
            "id": task.get("id"),
            "program": task.get("command", {}).get("program"),
            "runtimeInSeconds": task.get("runtimeInSeconds"),
            "avgCPU": task.get("avgCPU"),
            "priority": task.get("priority"),
            "machine": task.get("machines", ["N/A"])[0]
        })

    # Create DataFrame and save to CSV
    df = pd.DataFrame(task_data)
    df.to_csv("executed_tasks_airrflow.csv", index=False)
    print(f"✅ Saved {len(df)} executed tasks to 'executed_tasks_airrflow.csv'")
