import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error

# === Load and preprocess ===
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\scientific-workflows\logical_task_dataset.csv')
for col in ['workflow_name', 'task_category', 'task_name']:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col])

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

# === Pick a test task ===
test_task_id = max(task_data, key=lambda k: len(task_data[k][0]))  # most samples
X_task, y_task = task_data[test_task_id]

if len(X_task) < 10:
    raise ValueError("Not enough data for testing.")

support_X, support_y = X_task[:5], y_task[:5]
query_X, query_y = X_task[5:], y_task[5:]

support_X = torch.tensor(support_X, dtype=torch.float32)
support_y = torch.tensor(support_y, dtype=torch.float32)
query_X = torch.tensor(query_X, dtype=torch.float32)
query_y = torch.tensor(query_y, dtype=torch.float32)

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

# === Load trained model ===
input_dim = X.shape[1]
model = TaskRegressor(input_dim)
model.load_state_dict(torch.load("reptile_meta_model_stable.pt"))
model.eval()

# === Fine-tune ===
optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
for _ in range(10):
    loss = nn.functional.mse_loss(model(support_X), support_y)
    if torch.isnan(loss):
        print("❌ NaN in fine-tuning loss.")
        exit()
    optimizer.zero_grad()
    loss.backward()
    torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
    optimizer.step()

# === Predict ===
with torch.no_grad():
    preds = model(query_X).numpy()

if np.isnan(preds).any():
    print("❌ NaNs in prediction!")
    print(preds)
    exit()

# === Evaluate ===
mae = mean_absolute_error(query_y.numpy(), preds)
print(f"✅ Tested on task ID: {test_task_id}")
print(f"📊 MAE on query set: {mae:.4f}")
