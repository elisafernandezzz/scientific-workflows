import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score
from statistics import mean, stdev
import matplotlib.pyplot as plt
import joblib
import pickle

# Load dataset - using physical task dataset
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\scientific-workflows\results\task_level_dataset_test.csv')
df = df.dropna()

# Extract workflow type
df['workflow_type'] = df['workflow_name'].apply(lambda x: str(x).split('-')[0])

df['workflow_type'] = df['workflow_type'].str.lower()

df['workflow_type'] = df['workflow_type'].replace({
    'workflow': 'srasearch'
})

# Encode categorical features (same as training)
categorical_cols = ['logical_task_name', 'task_category']
for col in categorical_cols:
    if col in df.columns:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])

# Add log-transformed features
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

# Define target and apply log transformation
target_column = 'physical_children_count'
df['target_log'] = np.log1p(df[target_column])

# Load metadata and scaler
try:
    with open('reptile_physical_metadata.pkl', 'rb') as f:
        metadata = pickle.load(f)
    available_features = metadata['features']
    input_dim = metadata['input_dim']
    print(f"Loaded metadata: {len(available_features)} features, input_dim={input_dim}")
except FileNotFoundError:
    # Fallback to manual feature definition
    available_features = [
        'log_input_size',  
        'task_category',
        'logical_children_count', 
        'logical_dependencies_count', 
        'total_input_file_sizes', 
        'input_file_count', 
    ]
    available_features = [f for f in available_features if f in df.columns]
    input_dim = len(available_features)
    print(f"Using fallback features: {available_features}")

# Prepare features
X_raw = df[available_features]

# Load scaler
try:
    scaler = joblib.load("reptile_physical_scaler.pkl")
    print("✅ Loaded scaler from training")
except FileNotFoundError:
    print("⚠️  Scaler not found, creating new one")
    scaler = StandardScaler()
    scaler.fit(X_raw)

X = scaler.transform(X_raw)
y_raw = df['target_log'].values

# Group data by workflow type
workflow_types = df['workflow_type'].values
task_data = {}
for i, wf_type in enumerate(workflow_types):
    task_data.setdefault(wf_type, []).append((X[i], y_raw[i]))

# Convert to numpy arrays
for task_name in task_data:
    X_task, y_task = zip(*task_data[task_name])
    task_data[task_name] = (np.array(X_task), np.array(y_task))


# Define model architecture (same as training)
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

# Load trained Reptile model
base_model = TaskRegressor(input_dim)
try:
    base_model.load_state_dict(torch.load("reptile_physical_meta_model.pt"))
    print("✅ Loaded trained Reptile model")
except FileNotFoundError:
    print("❌ Trained model not found! Please run training first.")
    exit(1)

base_model.eval()

# Check workflow type distribution
task_counts = df['workflow_type'].value_counts()

# Evaluate across multiple tasks with few-shot learning
mae_scores = []
r2_scores = []
evaluated_tasks = []

min_samples_for_eval = 15
support_size = 5  # Number of samples for few-shot adaptation
adaptation_steps = 20

for task_id, (X_task, y_task) in task_data.items():
    if len(X_task) < min_samples_for_eval:
        continue

    # Split into support and query sets
    indices = np.random.permutation(len(X_task))
    support_idx = indices[:support_size]
    query_idx = indices[support_size:]
    
    support_X_np, query_X_np = X_task[support_idx], X_task[query_idx]
    support_y_np, query_y_np = y_task[support_idx], y_task[query_idx]

    if len(query_X_np) < 3:  # Need enough query samples
        continue

    # Convert to tensors
    support_X = torch.tensor(support_X_np, dtype=torch.float32)
    support_y = torch.tensor(support_y_np, dtype=torch.float32)
    query_X = torch.tensor(query_X_np, dtype=torch.float32)
    query_y = torch.tensor(query_y_np, dtype=torch.float32)

    # Clone and adapt model
    model = TaskRegressor(input_dim)
    model.load_state_dict(base_model.state_dict())
    model.train()  # Enable training mode for adaptation

    # Few-shot adaptation on support set
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005)
    
    for step in range(adaptation_steps):
        optimizer.zero_grad()
        pred_support = model(support_X)
        loss = nn.functional.mse_loss(pred_support, support_y)
        
        if torch.isnan(loss):
            print(f"NaN loss during adaptation for {task_id}")
            break
            
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()

    # Evaluate on query set
    model.eval()
    with torch.no_grad():
        preds = model(query_X).numpy()

    if np.isnan(preds).any():
        print(f"NaN predictions for task {task_id}")
        continue

    # Calculate metrics
    mae = mean_absolute_error(query_y.numpy(), preds)
    r2 = r2_score(query_y.numpy(), preds)
    
    mae_scores.append(mae)
    r2_scores.append(r2)
    evaluated_tasks.append(task_id)

# Report overall results
print(f"\n✅ Evaluated Reptile on {len(evaluated_tasks)} workflow types")
if mae_scores:
    print(f"📊 Average MAE: {mean(mae_scores):.3f}")
    print(f"📉 Std Dev MAE: {stdev(mae_scores) if len(mae_scores) > 1 else 0:.3f}")
    print(f"📊 Average R²: {mean(r2_scores):.3f}")
    print(f"📉 Std Dev R²: {stdev(r2_scores) if len(r2_scores) > 1 else 0:.3f}")
else:
    print("❌ No tasks could be evaluated")

# Save detailed results
if mae_scores:
    results_df = pd.DataFrame({
        "workflow_type": evaluated_tasks,
        "mae": mae_scores,
        "r2": r2_scores
    })
    results_df.to_csv("reptile_physical_evaluation.csv", index=False)
    print("📁 Saved results to reptile_physical_evaluation.csv")


# Per-workflow-type detailed evaluation (using full datasets)
print(f"\n📋 Detailed per-workflow-type analysis:")

detailed_results = []
for wf_type in df['workflow_type'].unique():
    df_wf = df[df['workflow_type'] == wf_type]
    
    if len(df_wf) < 5:
        continue
        
    X_wf = df_wf[available_features]
    y_wf = df_wf['target_log']
    
    # Use base model without adaptation for this analysis
    base_model.eval()
    X_wf_tensor = torch.tensor(scaler.transform(X_wf), dtype=torch.float32)
    
    with torch.no_grad():
        y_pred_tensor = base_model(X_wf_tensor)
        y_wf_pred = y_pred_tensor.cpu().numpy().flatten()

    mae = mean_absolute_error(y_wf, y_wf_pred)
    r2 = r2_score(y_wf, y_wf_pred) if len(y_wf) > 1 else np.nan

    detailed_results.append({
        'Workflow Type': wf_type,
        'Physical Task Count': len(df_wf),
        'MAE': round(mae, 3),
        'R²': round(r2, 3) if not np.isnan(r2) else np.nan,
        'Unique Logical Tasks': df_wf['logical_task_name'].nunique() if 'logical_task_name' in df_wf.columns else 0
    })

# Sort and display results
detailed_results_df = pd.DataFrame(detailed_results)
if not detailed_results_df.empty:
    detailed_results_df_sorted = detailed_results_df.copy()
    detailed_results_df_sorted['R²_for_sort'] = detailed_results_df_sorted['R²'].fillna(-999)
    detailed_results_df_sorted = detailed_results_df_sorted.sort_values(by='R²_for_sort', ascending=False)
    detailed_results_df_sorted = detailed_results_df_sorted.drop('R²_for_sort', axis=1)
    
    print(detailed_results_df_sorted)
    
    # Grouping analysis
    def categorize_task_count(n):
        if n < 50:
            return 'Small (<50)'
        elif 50 <= n <= 100:
            return 'Medium-small (50–100)'
        elif 101 <= n <= 200:
            return 'Medium (101–200)'
        else:
            return 'Large (>200)'

    detailed_results_df['Task Count Bin'] = detailed_results_df['Physical Task Count'].apply(categorize_task_count)

    grouped_metrics = detailed_results_df.groupby('Task Count Bin').agg({
        'R²': 'mean',
        'MAE': 'mean',
        'Physical Task Count': 'count'
    }).rename(columns={'Physical Task Count': 'Workflow Count'}).reset_index()

    print(f"\n📊 Grouped metrics by task count:")
    print(grouped_metrics)

    # Plot grouped results
    if len(grouped_metrics) > 0:
        fig, ax1 = plt.subplots(figsize=(10, 6))

        color_r2 = 'tab:blue'
        ax1.set_xlabel('Physical Task Count Bin')
        ax1.set_ylabel('Average R²', color=color_r2)
        ax1.bar(grouped_metrics['Task Count Bin'], grouped_metrics['R²'], 
                color=color_r2, alpha=0.6, label='R²')
        ax1.tick_params(axis='y', labelcolor=color_r2)
        ax1.set_ylim(-1, 1)

        ax2 = ax1.twinx()
        color_mae = 'tab:red'
        ax2.set_ylabel('Average MAE', color=color_mae)
        ax2.plot(grouped_metrics['Task Count Bin'], grouped_metrics['MAE'], 
                 color=color_mae, marker='o', label='MAE')
        ax2.tick_params(axis='y', labelcolor=color_mae)

        plt.title(f'Reptile Model - Average R² and MAE by Physical Task Count Bin')
        fig.tight_layout()
        plt.grid(True, alpha=0.3)
        plt.show()