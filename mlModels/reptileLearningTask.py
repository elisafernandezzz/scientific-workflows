import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from copy import deepcopy
import joblib

# Load dataset - using physical task dataset
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\scientific-workflows\results\task_level_dataset_test.csv')
df = df.dropna()

# Extract workflow type for meta-learning tasks
df['workflow_type'] = df['workflow_name'].apply(lambda x: str(x).split('-')[0])

# Encode categorical features
categorical_cols = ['logical_task_name', 'task_category']
for col in categorical_cols:
    if col in df.columns:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])

# Add log-transformed size features
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

# Define target variable
target_column = 'physical_children_count'

# Apply log transformation to target to handle skewness
df['target_log'] = np.log1p(df[target_column])

# Prepare features for physical task prediction
selected_features = [
    'log_input_size',
    'task_category', 
    'logical_children_count',
    'logical_dependencies_count',
    'total_input_file_sizes',
    'input_file_count',
]

# Filter features that exist in dataframe
available_features = [f for f in selected_features if f in df.columns]

X_raw = df[available_features].copy()
y_raw = df['target_log'].values  # Use log-transformed target

# Normalize numerical features
scaler = StandardScaler()
X = scaler.fit_transform(X_raw)

# Save the scaler for use in evaluation
joblib.dump(scaler, "reptile_physical_scaler.pkl")
print("📦 Scaler saved as reptile_physical_scaler.pkl")

# Group data by workflow type for meta-learning
# Each workflow type becomes a "task" in meta-learning
workflow_types = df['workflow_type'].values
task_data = {}
for i, wf_type in enumerate(workflow_types):
    task_data.setdefault(wf_type, []).append((X[i], y_raw[i]))

# Convert to numpy arrays and filter out tasks with too few samples
min_samples_per_task = 10
filtered_task_data = {}
for task_name, task_samples in task_data.items():
    if len(task_samples) >= min_samples_per_task:
        X_task, y_task = zip(*task_samples)
        filtered_task_data[task_name] = (np.array(X_task), np.array(y_task))


# Define model architecture
class TaskRegressor(nn.Module):
    def __init__(self, input_dim):
        super(TaskRegressor, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
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

# Reptile meta-learning step
def reptile_step(meta_model, meta_tasks, inner_steps=10, meta_lr=0.001, inner_lr=0.01):
    task_updates = 0
    
    for task_name, (X_task, y_task) in meta_tasks.items():
        if len(X_task) < 8:  # Need minimum samples for support/query split
            continue

        # Random split into support and remaining data
        indices = np.random.permutation(len(X_task))
        support_size = min(5, len(X_task) // 2)
        support_idx = indices[:support_size]
        
        support_X = torch.tensor(X_task[support_idx], dtype=torch.float32)
        support_y = torch.tensor(y_task[support_idx], dtype=torch.float32)

        # Clone meta model for task-specific adaptation
        task_model = deepcopy(meta_model)
        optimizer = torch.optim.Adam(task_model.parameters(), lr=inner_lr)

        # Task-specific training (inner loop)
        for step in range(inner_steps):
            optimizer.zero_grad()
            predictions = task_model(support_X)
            loss = F.mse_loss(predictions, support_y)
            
            if torch.isnan(loss):
                print(f"NaN loss in task {task_name}, skipping...")
                break
                
            loss.backward()
            torch.nn.utils.clip_grad_norm_(task_model.parameters(), 1.0)
            optimizer.step()

        # Meta-update: interpolate between meta model and adapted model
        with torch.no_grad():
            for meta_param, task_param in zip(meta_model.parameters(), task_model.parameters()):
                meta_param.data += meta_lr * (task_param.data - meta_param.data)
        
        task_updates += 1

    return meta_model, task_updates

# Initialize meta-model
input_dim = X.shape[1]
meta_model = TaskRegressor(input_dim)
meta_model.apply(init_weights)

# Training loop
num_epochs = 50
print(f"\n🚀 Starting Reptile meta-learning for {num_epochs} epochs...")

for epoch in range(num_epochs):
    meta_model, updates = reptile_step(
        meta_model, 
        filtered_task_data, 
        inner_steps=15,
        meta_lr=0.001,
        inner_lr=0.01
    )
    
    if (epoch + 1) % 10 == 0:
        print(f"✅ Epoch {epoch+1}/{num_epochs} completed. Tasks updated: {updates}")

# Stabilize model parameters
with torch.no_grad():
    for param in meta_model.parameters():
        param.clamp_(-5, 5)

# Save the trained model
model_path = "reptile_physical_meta_model.pt"
torch.save(meta_model.state_dict(), model_path)
print(f"🎉 Model saved as {model_path}")

# Save additional metadata
metadata = {
    'input_dim': input_dim,
    'features': available_features,
    'target_column': target_column,
    'num_tasks': len(filtered_task_data),
    'total_samples': len(df)
}

import pickle
with open('reptile_physical_metadata.pkl', 'wb') as f:
    pickle.dump(metadata, f)

print("📋 Metadata saved as reptile_physical_metadata.pkl")
print(f"Meta-learning completed with {len(filtered_task_data)} workflow types")