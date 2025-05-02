import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# Load dataset
df = pd.read_csv("logical_task_dataset.csv")
df = df.dropna()

# Select features and target
features = ['children_count', 'input_file_count', 'output_file_count']
X = df[features]
y = df['instance_count']

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Linear Regression
model = LinearRegression()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Evaluate
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("Linear Regression Evaluation:")
print(f"- MSE: {mse:.2f}")
print(f"- MAE: {mae:.2f}")
print(f"- R²: {r2:.2f}")

# Generate one plot per feature
for feature in features:
    plt.figure(figsize=(8, 6))
    sns.regplot(x=feature, y='instance_count', data=df, scatter_kws={'alpha': 0.6}, line_kws={'color': 'red'})
    plt.xlabel(feature.replace('_', ' ').title())
    plt.ylabel("Instance Count")
    plt.title(f"Instance Count vs {feature.replace('_', ' ').title()}")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# ---------- Optional: Correlation ----------
corr = df['total_input_file_sizes'].corr(df['instance_count'])
print(f"\nCorrelation between total input file sizes and instance count: {corr:.2f}")


