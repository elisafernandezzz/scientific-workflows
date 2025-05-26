import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from copy import deepcopy

# Load dataset
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\scientific-workflows\logical_task_dataset.csv')
# Encode categorical features
for col in ['workflow_name', 'task_category', 'task_name']:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col])

X_raw = df.drop(columns=['instance_count'])
y_raw = df['instance_count'].values

# Normalize numerical features
scaler = StandardScaler()
X = scaler.fit_transform(X_raw)

# Group rows by task_name
task_names = df['task_name'].values
task_data = {}
for i, task in enumerate(task_names):
    task_data.setdefault(task, []).append((X[i], y_raw[i]))

# Convert to numpy arrays
for task in task_data:
    X_task, y_task = zip(*task_data[task])
    task_data[task] = (np.array(X_task), np.array(y_task))

# Define model
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

# Weight initialization
def init_weights(m):
    if isinstance(m, nn.Linear):
        nn.init.xavier_uniform_(m.weight)
        nn.init.zeros_(m.bias)

# Reptile step
def reptile_step(meta_model, meta_tasks, inner_steps=5, meta_lr=0.001, inner_lr=0.001):
    for X_task, y_task in meta_tasks:
        if len(X_task) < 6:
            continue

        indices = np.random.permutation(len(X_task))
        support_idx = indices[:3]
        support_X = torch.tensor(X_task[support_idx], dtype=torch.float32)
        support_y = torch.tensor(y_task[support_idx], dtype=torch.float32)

        task_model = deepcopy(meta_model)
        optimizer = torch.optim.SGD(task_model.parameters(), lr=inner_lr)

        for _ in range(inner_steps):
            loss = F.mse_loss(task_model(support_X), support_y)
            if torch.isnan(loss):
                print("NaN in loss, skipping this task.")
                return meta_model
            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(task_model.parameters(), 1.0)
            optimizer.step()

        # Meta-update
        for param, task_param in zip(meta_model.parameters(), task_model.parameters()):
            param.data += meta_lr * (task_param.data - param.data)

    return meta_model

# Training loop
input_dim = X.shape[1]
meta_model = TaskRegressor(input_dim)
meta_model.apply(init_weights)
meta_tasks = list(task_data.values())

for epoch in range(20):
    meta_model = reptile_step(meta_model, meta_tasks)
    print(f"✅ Epoch {epoch+1}/20 completed.")

# Clamp model parameters to prevent instability
with torch.no_grad():
    for p in meta_model.parameters():
        p.clamp_(-10, 10)

# Save model
torch.save(meta_model.state_dict(), "reptile_meta_model_stable.pt")
print("🎉 Stable model saved as reptile_meta_model_stable.pt")
