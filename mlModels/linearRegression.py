# src/model_training.py

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import joblib  # To save the model

from mlModels.preprocessing import load_dataset, preprocess_data

def train_linear_regression(data_path, target_column, model_save_path):
    """
    Train a Linear Regression model.

    Args:
        data_path (str): Path to the cleaned dataset
        target_column (str): Name of the target column
        model_save_path (str): Where to save the trained model
    """
    # Load and preprocess data
    df = load_dataset(data_path)
    df = preprocess_data(df)
    
    # Separate features and target
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize and train model
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # Evaluate
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"Model Evaluation:\n- MSE: {mse:.2f}\n- R²: {r2:.2f}")
    
    # Save model
    joblib.dump(model, model_save_path)
    print(f"Model saved to {model_save_path}")

def preprocess_data(df):
    """
    Preprocess the dataset (basic cleaning).
    """
    df = df.dropna()  # Drop missing values
    df = df.reset_index(drop=True)

    # Keep only numeric columns
    df = df.select_dtypes(include=['number'])

    return df


if __name__ == "__main__":
    # Example usage
    train_linear_regression(
        data_path="logical_task_dataset.csv",     
        target_column="instance_count",                 
        model_save_path="models/linear_regression_model.pkl"
    )
