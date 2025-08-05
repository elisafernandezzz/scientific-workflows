import os
import json

base_path = "./scientific-workflows/WfInstances"
json_count = 0
all_jsons = []

for root, _, files in os.walk(base_path):
    for file in files:
        if file.endswith('.json'):
            json_count += 1
            all_jsons.append(os.path.join(root, file))

print(f"✅ Found {json_count} JSON files in nested folders.")

workflow_names = set()
for path in all_jsons:
    with open(path) as f:
        data = json.load(f)
        workflow_names.add(data.get("name", os.path.basename(path)))

print(f"🔍 Unique workflow names from JSONs: {len(workflow_names)}")

for wf_name in workflow_names:
    if "genome" in wf_name.lower():
        print(wf_name)


