import os
import json
from collections import defaultdict

# Root directory containing all workflow JSON files
root_dir = "scientific-workflows/WfInstances"  # 🔁 Replace with your actual path

# Store task counts grouped by workflow type
workflow_type_counts = defaultdict(list)

# Recursive walk to find and process each .json file
for subdir, _, files in os.walk(root_dir):
    for file in files:
        if file.endswith(".json"):
            filepath = os.path.join(subdir, file)
            try:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)

                tasks = data.get("workflow", {}).get("specification", {}).get("tasks", [])
                logical_task_names = set(task["name"] for task in tasks if "name" in task)
                count = len(logical_task_names)

                # Extract workflow type from filename (first segment before "-")
                workflow_type = file.split("-")[0]
                workflow_type_counts[workflow_type].append(count)

                print(f"{file}: {count} logical tasks")

            except Exception as e:
                print(f"Error processing {filepath}: {e}")

# Summary: compute average per workflow type
print("\n--- Average Logical Tasks by Workflow Type ---")
for workflow_type, counts in sorted(workflow_type_counts.items()):
    avg = sum(counts) / len(counts)
    print(f"{workflow_type}: {avg:.2f} average logical tasks")
