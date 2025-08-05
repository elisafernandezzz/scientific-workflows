import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor
from sklearn.preprocessing import MultiLabelBinarizer

# Load and clean data
df = pd.read_csv(r'C:\Users\elisa\Desktop\4 ING INF ELISA\BACHELOR THESIS\GIT\logical_task_dataset_test.csv')
df = df.dropna()

# Encode categorical features
for col in ['task_category', 'task_name']:
    if col in df.columns:
        df[col] = LabelEncoder().fit_transform(df[col])

# Log-transform input/output sizes
df['log_input_size'] = np.log1p(df['total_input_file_sizes'])
df['log_output_size'] = np.log1p(df['total_output_file_sizes'])

df['workflow_type'] = df['workflow_name'].apply(lambda x: str(x).split('-')[0])

# Features and target
features = [
    #'children_count',
    'total_input_file_sizes',
    #'total_output_file_sizes',
    'log_input_size',
    #'log_output_size',
    'task_category',
    #'task_name',
    #'top_input_file_formats',
    'logical_children_count',
    'logical_dependencies_count'
]
X = df[features]
y = df['instance_count']

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train XGBoost Regressor
model = XGBRegressor(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    objective='reg:squarederror',
    random_state=42
)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Evaluate
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("\nXGBoost Regression Results:")
print(f"- MSE: {mse:.2f}")
print(f"- MAE: {mae:.2f}")
print(f"- R²: {r2:.2f}")

# Feature importance plot
importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)

plt.figure(figsize=(8, 6))
sns.barplot(x=importances.values, y=importances.index)
plt.title("Feature Importances (XGBoost)")
plt.tight_layout()
plt.show()

# Plot: True vs Predicted
plt.figure(figsize=(8, 6))
sns.scatterplot(x=y_test, y=y_pred, alpha=0.6)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
plt.xlabel("True Instance Count")
plt.ylabel("Predicted Instance Count")
plt.title("True vs Predicted Instance Count (XGBoost)")
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot: Residuals
residuals = y_test - y_pred
plt.figure(figsize=(8, 6))
sns.histplot(residuals, kde=True, bins=30)
plt.title("Residuals Distribution (XGBoost)")
plt.xlabel("Prediction Error")
plt.tight_layout()
plt.show()

# === Per-workflow-type Evaluation (Safe for single-row workflows) ===
results = []

for wf_type in df['workflow_type'].unique():
    df_wf = df[df['workflow_type'] == wf_type]
    X_wf = df_wf[features]
    y_wf = df_wf['instance_count']
    y_wf_pred = model.predict(X_wf)

    mae = mean_absolute_error(y_wf, y_wf_pred)
    r2 = r2_score(y_wf, y_wf_pred) if len(y_wf) > 1 else np.nan  # Prevent undefined warning

    results.append({
        'Workflow Type': wf_type,
        'MAE': round(mae, 3),
        'R²': round(r2, 3) if not np.isnan(r2) else np.nan
    })

# === Display the sorted result ===
xgb_results_df = pd.DataFrame(results).sort_values(by='R²', ascending=False)
print("\nPer-workflow-type results (XGBoost):")
print(xgb_results_df)
