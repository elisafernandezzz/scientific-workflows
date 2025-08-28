import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor
from sklearn.preprocessing import MultiLabelBinarizer

# Load and clean - using physical task dataset
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\scientific-workflows\results\task_level_dataset_test.csv')
df = df.dropna()

# Encode categorical features
categorical_cols = ['logical_task_name', 'task_category']
for col in categorical_cols:
    if col in df.columns:
        df[col] = LabelEncoder().fit_transform(df[col])

# Log-transform input/output sizes
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

# Extract workflow type from workflow name
df['workflow_type'] = df['workflow_name'].apply(lambda x: str(x).split('-')[0])

df['workflow_type'] = df['workflow_type'].str.lower()

df['workflow_type'] = df['workflow_type'].replace({
    'workflow': 'srasearch'
})

target_column = 'physical_children_count'

# Features for physical task prediction
features = [
    'log_input_size',
    'task_category',
    'logical_children_count',
    'logical_dependencies_count',
    'total_input_file_sizes',
    'input_file_count',
]

# Filter features that exist in the dataframe
available_features = [f for f in features if f in df.columns]

X = df[available_features]
y = df[target_column]


# Check for extreme outliers
outlier_threshold = y.quantile(0.95)

# Consider log-transforming the highly skewed target
y_log = np.log1p(y)  # log(1 + x) to handle 0 values

# Use log-transformed target
y = y_log
original_target_column = target_column  # Keep original for dataframe access
target_column_display = target_column + " (log-transformed)"  # For display only

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train XGBoost Regressor with better regularization
model = XGBRegressor(
    n_estimators=200,        
    max_depth=3,             
    learning_rate=0.03,      
    subsample=0.8,            
    colsample_bytree=0.6,    
    reg_alpha=2,            
    reg_lambda=3,            
    min_child_weight=5,      
    random_state=42
)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Predict on training set
y_train_pred = model.predict(X_train)

# Evaluate on training
train_mse = mean_squared_error(y_train, y_train_pred)
train_mae = mean_absolute_error(y_train, y_train_pred)
train_r2 = r2_score(y_train, y_train_pred)

# Evaluate on test
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"\n📊 XGBoost Training Set Evaluation (Predicting {target_column_display}):")
print(f"- MSE: {train_mse:.2f}")
print(f"- MAE: {train_mae:.2f}")
print(f"- R²: {train_r2:.2f}")

print(f"\nXGBoost Test Set Results (Predicting {target_column_display}):")
print(f"- MSE: {mse:.2f}")
print(f"- MAE: {mae:.2f}")
print(f"- R²: {r2:.2f}")

# Feature importance plot
importances = pd.Series(model.feature_importances_, index=available_features).sort_values(ascending=False)

plt.figure(figsize=(10, 6))
sns.barplot(x=importances.values, y=importances.index)
plt.title(f"Feature Importances for Predicting {target_column_display} (XGBoost)", fontsize=10)
plt.xlabel("Coefficient", fontsize=14)
plt.ylabel("Features", fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.tight_layout()
plt.show()

# Plot: True vs Predicted
plt.figure(figsize=(8, 6))
sns.scatterplot(x=y_test, y=y_pred, alpha=0.6)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
plt.xlabel(f"True {target_column_display}")
plt.ylabel(f"Predicted {target_column_display}")
plt.title(f"True vs Predicted {target_column_display} (XGBoost)")
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot: Residuals
residuals = y_test - y_pred
plt.figure(figsize=(8, 6))
sns.histplot(residuals, kde=True, bins=30)
plt.title(f"Residuals Distribution for {target_column_display} (XGBoost)")
plt.xlabel("Prediction Error")
plt.tight_layout()
plt.show()

# Per-workflow-type Evaluation
results = []

for wf_type in df['workflow_type'].unique():
    df_wf = df[df['workflow_type'] == wf_type]
    if len(df_wf) < 5:  # Skip types with too little data
        continue
    
    X_wf = df_wf[available_features]
    y_wf = np.log1p(df_wf[original_target_column])  # Apply same log transform
    y_wf_pred = model.predict(X_wf)
    
    physical_task_count = len(df_wf)  # Count of physical tasks
    mae = mean_absolute_error(y_wf, y_wf_pred)
    r2 = r2_score(y_wf, y_wf_pred) if len(y_wf) > 1 else np.nan 

    results.append({
        'Workflow Type': wf_type,
        'MAE': round(mae, 3),
        'R²': round(r2, 3) if not np.isnan(r2) else np.nan,
        'Physical Task Count': physical_task_count,
        'Unique Logical Tasks': df_wf['logical_task_name'].nunique()
    })

# Display the sorted result
xgb_results_df = pd.DataFrame(results)
# Handle NaN values by replacing with a very low value for sorting, then restore
xgb_results_df_sorted = xgb_results_df.copy()
xgb_results_df_sorted['R²_for_sort'] = xgb_results_df_sorted['R²'].fillna(-999)
xgb_results_df_sorted = xgb_results_df_sorted.sort_values(by='R²_for_sort', ascending=False)
xgb_results_df_sorted = xgb_results_df_sorted.drop('R²_for_sort', axis=1)
print(f"\nPer-workflow-type results for {target_column_display} prediction (XGBoost):")
print(xgb_results_df_sorted)

# Convert to DataFrame for grouping
results_df = pd.DataFrame(results)

# Define binning function for physical tasks
def categorize_task_count(n):
    if n < 50:
        return 'Small (<50)'
    elif 50 <= n <= 100:
        return 'Medium-small (50–100)'
    elif 101 <= n <= 200:
        return 'Medium (101–200)'
    else:
        return 'Large (>200)'

# Apply bins to the results
results_df['Task Count Bin'] = results_df['Physical Task Count'].apply(categorize_task_count)

# Group and compute metrics
grouped_metrics = results_df.groupby('Task Count Bin').agg({
    'R²': 'mean',
    'MAE': 'mean',
    'Physical Task Count': 'count'
}).rename(columns={'Physical Task Count': 'Workflow Count'}).reset_index()

# Plot bar chart of average R² by bin
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot R² on left axis
color_r2 = 'tab:blue'
ax1.set_xlabel('Physical Task Count Bin')
ax1.set_ylabel('Average R²', color=color_r2)
ax1.bar(grouped_metrics['Task Count Bin'], grouped_metrics['R²'], color=color_r2, alpha=0.6, label='R²')
ax1.tick_params(axis='y', labelcolor=color_r2)
ax1.set_ylim(-1, 1)

# Create second y-axis for MAE
ax2 = ax1.twinx()
color_mae = 'tab:red'
ax2.set_ylabel('Average MAE', color=color_mae)
ax2.plot(grouped_metrics['Task Count Bin'], grouped_metrics['MAE'], color=color_mae, marker='o', label='MAE')
ax2.tick_params(axis='y', labelcolor=color_mae)

# Title and layout
plt.title(f'Average R² and MAE by Physical Task Count Bin (Predicting {target_column_display})')
fig.tight_layout()
plt.grid(True)
plt.show()

# Print table
print("\nGrouped metrics by physical task count:")
print(grouped_metrics)

# Additional analysis: Physical vs Logical task relationship
print(f"\nPhysical Task Analysis Summary:")
print(f"- Total physical tasks: {len(df)}")
print(f"- Unique logical tasks: {df['logical_task_name'].nunique()}")
print(f"- Unique workflows: {df['workflow_name'].nunique()}")