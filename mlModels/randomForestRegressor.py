import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

# Load and clean data
df = pd.read_csv("logical_task_dataset.csv")
df = df.dropna()

# Encode categorical features
for col in ['workflow_name', 'task_category', 'task_name']:
    if col in df.columns:
        df[col] = LabelEncoder().fit_transform(df[col])

# Features and target
features = [
    'children_count',
    'total_input_file_sizes',
    'total_output_file_sizes',
]
X = df[features]
y = df['instance_count']

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest
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

# Feature importance plot
importances = pd.Series(model.feature_importances_, index=features)
importances = importances.sort_values(ascending=False)

plt.figure(figsize=(8, 6))
sns.barplot(x=importances.values, y=importances.index)
plt.title("Feature Importances (Random Forest)")
plt.tight_layout()
plt.show()

# Plot each feature vs instance_count
for feature in features:
    plt.figure(figsize=(8, 6))
    sns.regplot(x=feature, y='instance_count', data=df,
                scatter_kws={'alpha': 0.6}, line_kws={'color': 'red'})
    plt.xlabel(feature.replace('_', ' ').title())
    plt.ylabel("Instance Count")
    plt.title(f"Instance Count vs {feature.replace('_', ' ').title()}")
    plt.grid(True)
    plt.tight_layout()
    plt.show()