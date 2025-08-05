import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# === Load dataset ===
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\logical_task_dataset_test.csv')
df = df.dropna()

# === Log-transform file size columns (to reduce skew) ===
df['log_input_size'] = df['total_input_file_sizes'].apply(lambda x: np.log1p(x))
df['log_output_size'] = df['total_output_file_sizes'].apply(lambda x: np.log1p(x))

# === Select features ===
features = [
    #'children_count',
    'input_file_count',
    'log_input_size',
    'logical_children_count',
    'logical_dependencies_count'
]
X = df[features]
y = df['instance_count']

# === Train/test split ===
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# === Train Linear Regression ===
model = LinearRegression()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# === Evaluate ===
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("Linear Regression Evaluation:")
print(f"- MSE: {mse:.2f}")
print(f"- MAE: {mae:.2f}")
print(f"- R²: {r2:.2f}")

# === Feature Importance ===
print("\nFeature Coefficients:")
for feat, coef in zip(features, model.coef_):
    print(f"- {feat}: {coef:.4f}")

# === Correlation Matrix ===
print("\nCorrelation Matrix:")
print(df[features + ['instance_count']].corr())

# === Regression Plots ===
for feature in features:
    plt.figure(figsize=(8, 6))
    sns.regplot(x=feature, y='instance_count', data=df, scatter_kws={'alpha': 0.6}, line_kws={'color': 'red'})

    # Update label if it's the log-transformed input
    xlabel = "Log(Input File Size)" if feature == 'log_input_size' else feature.replace('_', ' ').title()
    plt.xlabel(xlabel)

    plt.ylabel("Instance Count")
    plt.title(f"Instance Count vs {xlabel}")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# === Residual Plot ===
residuals = y_test - y_pred
plt.figure(figsize=(8,6))
sns.histplot(residuals, bins=30, kde=True)
plt.title("Residuals Distribution")
plt.xlabel("Prediction Error")
plt.grid(True)
plt.show()

# === Optional: Group by higher-level workflow type ===
def extract_workflow_type(workflow_name):
    name_lower = workflow_name.lower()

    # Specific matches first
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

    # Fallback: try first word of name
    return name_lower.split('-')[0]

df['workflow_type'] = df['workflow_name'].apply(extract_workflow_type)

# === Per-workflow-type Evaluation ===
print("\n=== Per-Workflow-Type Performance (Linear Regression) ===")
results = []

for wf_type in df['workflow_type'].unique():
    df_wf = df[df['workflow_type'] == wf_type]
    X_wf = df_wf[features]
    y_wf = df_wf['instance_count']
    y_wf_pred = model.predict(X_wf)

    mae_wf = mean_absolute_error(y_wf, y_wf_pred)
    r2_wf = r2_score(y_wf, y_wf_pred)

    results.append({
        'Workflow Type': wf_type,
        'MAE': round(mae_wf, 3),
        'R²': round(r2_wf, 3)
    })

results_df = pd.DataFrame(results)
print(results_df.sort_values(by='R²', ascending=False))
