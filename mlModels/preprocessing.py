import pandas as pd
import os

def load_dataset(dataset_path):
    """
    Loads a dataset from a given CSV path.
    
    Args:
        dataset_path (str): Path to the dataset file (.csv)
    
    Returns:
        pd.DataFrame: Loaded dataset
    """
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset not found at {dataset_path}")
    
    df = pd.read_csv(dataset_path)
    return df

def preprocess_data(df):
    """
    Preprocess the dataset (basic cleaning).
    
    Args:
        df (pd.DataFrame): Raw dataset
    
    Returns:
        pd.DataFrame: Cleaned dataset
    """
    # Example cleaning steps:
    df = df.dropna()            # Drop rows with missing values
    df = df.reset_index(drop=True)  # Reset index after dropping

    # Optional: additional feature engineering could go here
    return df
