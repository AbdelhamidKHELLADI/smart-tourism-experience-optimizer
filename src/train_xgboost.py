import os
import mlflow
import mlflow.xgboost
from mlflow.models import infer_signature
from sklearn.model_selection import train_test_split, GridSearchCV
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error
from utils.s3_utils import read_from_s3
import boto3

import logging
logging.basicConfig(
    filename="logs/training.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


DATA_PATH = os.getenv("DATA_PATH", "preprocessed.csv")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "Tourism_Presence_Prediction")
REGISTERED_MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "TourismPresenceXGB")
S3_BUCKET = os.getenv("TOURISM_BUCKET")   
S3_KEY = "models/xgb.pkl"

test_size = 0.3
cv = 3


df=read_from_s3(S3_BUCKET,DATA_PATH)

X = df[[
    "Month_Num", "mobility_index", "weather_score",
    "temperature_2m_mean", "cloud_cover_mean", "snowfall_sum", "snowy_day"
] + [col for col in df.columns if col.startswith("region_")]]

y = df["tourism_index"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=test_size, shuffle=False
)


xgb = XGBRegressor(
    objective='reg:squarederror',
    random_state=42,
    n_jobs=-1
)

param_grid = {
    'n_estimators': [200, 600, 800],
    'learning_rate': [0.003, 0.01, 0.03],
    'max_depth': [3, 5, 7],
    'subsample': [0.7, 0.9, 1.0],
    'colsample_bytree': [0.7, 0.9, 1.0]
}

grid_search = GridSearchCV(
    estimator=xgb,
    param_grid=param_grid,
    scoring='r2',
    cv=cv,
    verbose=2,
    n_jobs=-1
)

mlflow.set_experiment(EXPERIMENT_NAME)


with mlflow.start_run(run_name="XGBoost_GridSearch") as run:
    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_
    best_params = grid_search.best_params_

    y_pred = best_model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)

    mlflow.log_params({"test_size": test_size, "cv": cv})
    mlflow.log_params(best_params)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mape", mape)
    mlflow.log_metric("mae", mae)

    os.makedirs("models", exist_ok=True)
    local_model_path = "models/xgb.pkl"
    best_model.save_model(local_model_path)


    s3 = boto3.client("s3")
    s3.upload_file(local_model_path, S3_BUCKET, S3_KEY)
    logging.info(f"Uploaded model to s3://{S3_BUCKET}/{S3_KEY}")

    signature = infer_signature(X_test, y_pred)
    mlflow.xgboost.log_model(
        xgb_model=best_model,
        artifact_path="xgboost_model",
        signature=signature,
        registered_model_name=REGISTERED_MODEL_NAME
    )
