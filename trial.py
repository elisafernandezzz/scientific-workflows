import pandas as pd

df = pd.read_csv("logical_task_dataset.csv")

# Basic info
print("Dataset shape:", df.shape)
print("\nMissing values per column:")
print(df.isnull().sum())

# Count zeros in key numeric columns
numeric_cols = [
    'instance_count', 'children_count', 'avg_children_count',
    'input_file_count', 'total_input_file_sizes',
    'output_file_count', 'total_output_file_sizes'
]
print("\nZero counts in numeric columns:")
for col in numeric_cols:
    print(f"{col}: {(df[col] == 0).sum()}")

# Optional: see how many rows have zero input/output file size but nonzero instances
suspicious = df[
    ((df['total_input_file_sizes'] == 0) | (df['total_output_file_sizes'] == 0)) &
    (df['instance_count'] > 0)
]
print(f"\nSuspicious rows (zero file size but nonzero instance count): {len(suspicious)}")
