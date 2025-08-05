import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error
from statistics import mean, stdev
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score

# === Load and preprocess ===
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\logical_task_dataset_test.csv')
for col in ['workflow_name', 'task_category', 'task_name']:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col])

# Add log-transformed features
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

X_raw = df.drop(columns=['instance_count'])
y_raw = df['instance_count'].values

scaler = StandardScaler()
X = scaler.fit_transform(X_raw)

task_names = df['task_name'].values
task_data = {}
for i, task in enumerate(task_names):
    task_data.setdefault(task, []).append((X[i], y_raw[i]))

for task in task_data:
    X_task, y_task = zip(*task_data[task])
    task_data[task] = (np.array(X_task), np.array(y_task))

# === Define model ===
class TaskRegressor(nn.Module):
    def __init__(self, input_dim):
        super(TaskRegressor, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, x):
        return self.model(x).squeeze()

# === Load trained Reptile model ===
input_dim = X.shape[1]
base_model = TaskRegressor(input_dim)
base_model.load_state_dict(torch.load("reptile_meta_model_stable.pt"))
base_model.eval()

# === Evaluate across multiple tasks ===
mae_scores = []
evaluated_tasks = []
r2_scores = []

for task_id, (X_task, y_task) in task_data.items():
    if len(X_task) < 10:
        continue

    support_X_np, query_X_np = X_task[:5], X_task[5:]
    support_y_np, query_y_np = y_task[:5], y_task[5:]

    if len(query_X_np) == 0:
        continue

    support_X = torch.tensor(support_X_np, dtype=torch.float32)
    support_y = torch.tensor(support_y_np, dtype=torch.float32)
    query_X = torch.tensor(query_X_np, dtype=torch.float32)
    query_y = torch.tensor(query_y_np, dtype=torch.float32)

    # Clone model
    model = TaskRegressor(input_dim)
    model.load_state_dict(base_model.state_dict())
    model.eval()

    # Fine-tune on support set
    optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
    for _ in range(10):
        loss = nn.functional.mse_loss(model(support_X), support_y)
        if torch.isnan(loss):
            break
        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()

    # Predict on query set
    with torch.no_grad():
        preds = model(query_X).numpy()

    if np.isnan(preds).any():
        continue

    # Plot predictions for a few tasks
    if task_id in [0, 1, 2]:
        plt.figure(figsize=(6, 6))
        plt.scatter(query_y.numpy(), preds, alpha=0.6)
        plt.plot([query_y.min(), query_y.max()],
                 [query_y.min(), query_y.max()],
                 'r--', label="Ideal")
        plt.xlabel("True Instance Count")
        plt.ylabel("Predicted Instance Count")
        plt.title(f"Reptile Prediction - Task {task_id}")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.show()

    # Evaluate
    mae = mean_absolute_error(query_y.numpy(), preds)
    mae_scores.append(mae)
    evaluated_tasks.append(task_id)

    r2 = r2_score(query_y.numpy(), preds)
    r2_scores.append(r2)

# === Report Results ===
print(f"\n✅ Evaluated Reptile on {len(evaluated_tasks)} task IDs.")
print(f"📊 Average MAE: {mean(mae_scores):.2f}")
print(f"📉 Std Dev MAE: {stdev(mae_scores):.2f}")
print(f"Average R2 Score: {mean(r2_scores):.3f}")
print(f"Std Dev R2 Score: {stdev(r2_scores):.3f}")

# === Save to CSV ===
results_df = pd.DataFrame({
    "task_id": evaluated_tasks,
    "mae": mae_scores
})
results_df.to_csv("reptile_evaluation_by_task.csv", index=False)
print("📁 Saved per-task MAE results to reptile_evaluation_by_task.csv")

# === Plot MAE per task ID ===
plt.figure(figsize=(10, 6))
plt.bar(results_df['task_id'].astype(str), results_df['mae'], color='skyblue')
plt.xticks(rotation=45, ha='right')
plt.xlabel("Task ID")
plt.ylabel("MAE")
plt.title("Reptile Model - MAE per Task ID")
plt.tight_layout()
plt.grid(True, axis='y', linestyle='--', alpha=0.6)
plt.show()