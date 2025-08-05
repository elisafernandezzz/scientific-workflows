import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

# Load and clean data
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\logical_task_dataset_test.csv')
df = df.dropna()

# Optional: Encode categorical features (not used in this model, but can be useful)
for col in ['task_category', 'task_name']:
    if col in df.columns:
        df[col] = LabelEncoder().fit_transform(df[col])

# Add log-transformed file sizes
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

df['workflow_type'] = df['workflow_name'].apply(lambda x: str(x).split('-')[0])

# Define features and target
features = [
    #'children_count',
    'total_input_file_sizes',
    #'total_output_file_sizes',
    'log_input_size',
    #'log_output_size',
    #'task_category'
    'logical_children_count',
    'logical_dependencies_count'

]
X = df[features]
y = df['instance_count']

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = RandomForestRegressor(n_estimators=150, max_depth=12, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Evaluate
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("\nRandom Forest Regression Results:")
print(f"- MSE: {mse:.2f}")
print(f"- MAE: {mae:.2f}")
print(f"- R²: {r2:.2f}")

# Feature importances
importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
plt.figure(figsize=(8, 6))
sns.barplot(x=importances.values, y=importances.index)
plt.title("Feature Importances (Random Forest)")
plt.tight_layout()
plt.show()

# Plot: True vs Predicted
plt.figure(figsize=(8, 6))
sns.scatterplot(x=y_test, y=y_pred, alpha=0.6)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
plt.xlabel("True Instance Count")
plt.ylabel("Predicted Instance Count")
plt.title("True vs Predicted Instance Count")
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot: Residuals distribution
residuals = y_test - y_pred
plt.figure(figsize=(8, 6))
sns.histplot(residuals, kde=True, bins=40)
plt.xlabel("Prediction Error")
plt.title("Residuals Distribution")
plt.tight_layout()
plt.show()

# === Optional: Group by higher-level workflow type ===
def extract_workflow_type(workflow_name):
    name_lower = workflow_name.lower()

    # Specific workflow patterns
    if '1000genome' in name_lower:
        return '1000genome'
    elif 'epigenomics' in name_lower or 'epigenome' in name_lower:
        return 'epigenomics'
    elif 'srasearch' in name_lower:
        return 'srasearch'
    elif 'workflow-test' in name_lower:
        return 'srasearch'
    elif 'montage' in name_lower:
        return 'montage'
    elif 'soykb' in name_lower:
        return 'soykb'
    elif 'seismology' in name_lower:
        return 'seismology'
    elif 'bwa' in name_lower:
        return 'bwa'
    elif 'blast' in name_lower:
        return 'blast'
    elif 'rnaseq' in name_lower:
        return 'rnaseq'
    elif 'chipseq' in name_lower:
        return 'chipseq'
    elif 'bacass' in name_lower:
        return 'bacass'
    elif 'airrflow' in name_lower:
        return 'airrflow'
    elif 'chain' in name_lower:
        return 'chain'
    elif 'atacseq' in name_lower:
        return 'atacseq'
    elif 'cutandrun' in name_lower:
        return 'cutandrun'
    elif 'methylseq' in name_lower:
        return 'methylseq'
    elif 'mag' in name_lower:
        return 'mag'
    elif 'hic' in name_lower:
        return 'hic'
    elif 'forkjoin' in name_lower:
        return 'forkjoin'
    elif 'fetchngs' in name_lower:
        return 'fetchngs'
    elif 'sarek' in name_lower:
        return 'sarek'
    elif 'taxprofiler' in name_lower:
        return 'taxprofiler'
    elif 'viralrecon' in name_lower:
        return 'viralrecon'
    elif 'cycles' in name_lower:
        return 'cycles'
    elif 'makeflow' in name_lower:
        return 'makeflow'

    # Default fallback
    return name_lower.split('-')[0]

# Apply type extraction
df['workflow_type'] = df['workflow_name'].apply(extract_workflow_type)

# === Per-Workflow-Type Performance for Random Forest ===
print("\n=== Per-Workflow-Type Performance (Random Forest) ===")
results = []

for wf_type in df['workflow_type'].unique():
    df_wf = df[df['workflow_type'] == wf_type]
    if len(df_wf) < 2:
        continue  # Skip types with too little data to evaluate

    X_wf = df_wf[features]
    y_wf = df_wf['instance_count']
    y_wf_pred = model.predict(X_wf)

    mae = mean_absolute_error(y_wf, y_wf_pred)
    r2 = r2_score(y_wf, y_wf_pred)

    results.append({
        'Workflow Type': wf_type,
        'MAE': round(mae, 3),
        'R²': round(r2, 3)
    })

rf_results_df = pd.DataFrame(results).sort_values(by='R²', ascending=False)
print(rf_results_df)