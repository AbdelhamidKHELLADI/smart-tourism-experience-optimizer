# Smart Tourism Experience Optimizer

An end-to-end project that ingests GTFS transit data, tourism movement data and weather forecasts to build weekly tourism presence forecasts per region in Trentino. The repository contains ETL scripts, a Streamlit dashboard, an AWS Lambda-based forecasting service, and CI/CD workflows to build and deploy the Lambda.

This README documents how to run the project locally, what the components are, environment variables and recommended next steps.

## Repository layout (important folders)

- `src/etl/` — ETL pipelines (GTFS, weather, tourism preprocessing)
- `src/lambda_package/` — Lambda function code + Dockerfile to build container image for AWS Lambda
- `src/dashboard/` — Streamlit dashboards and assets
- `dags/` — Airflow DAGs (yearly pipeline)
- `data/` — (expected) input and output CSVs used by dashboard and tests
- `mlruns/` — local MLflow models and artifacts (optional, typically large)
- `.github/workflows/` — CI/CD workflows (build & push Lambda, upload dashboard)


## Quickstart — development (local)

Requirements:
- Python 3.11
- Docker (optional: to build lambda image)
- AWS CLI configured (if you want to run AWS steps locally)

1) Create a virtual environment and install dependencies (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r src/dashboard/requirements.txt
# and any other requirements you need for ETL/testing
```

2) Run Streamlit dashboards locally

```bash
# From repository root
streamlit run src/dashboard/Weekly_Forecast.py
# or
streamlit run src/dashboard/Home.py
```

3) Run ETL locally (quick test)

```bash
python src/etl/main_etl.py
```

Notes: many ETL scripts expect environment variables for S3 bucket names and AWS credentials. See the Environment Variables section below.


## Build & Deploy Lambda (CI/CD)

The repository includes a GitHub Actions workflow `.github/workflows/update_lambda.yml` that builds the Lambda container image and deploys it to AWS ECR and Lambda. The workflow is configured to run on pushes to `src/lambda_package/**`.

Secrets required in GitHub repository settings:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- (optional) `LAMBDA_EXEC_ROLE_ARN` if you want automation to create the function

Important: The workflow builds an image for `linux/amd64` and pushes it to the account's ECR. Make sure the function exists (or use the script to create it).


## Environment variables used by code

Several modules rely on environment variables; set these in your environment or in your deployment system:

- `TOURISM_BUCKET` — S3 bucket used for reading/writing preprocessed and prediction CSVs
- `FORECAST_CSV_PATH` — path within the bucket or local path for predictions.csv (default `predictions.csv`)
- `REGIONS_BOUNDARIES_PATH` — JSON of region bounding boxes used by the forecast
- `EXISTING_PREDS_PATH` — path to existing predictions CSV file
- `PREPROCESSED_PATH` — path to preprocessed mobility CSV
- `SCALING_PARAMS_PATH` — JSON path for scaling parameters used in weather scoring
- `WEEKLY_DATA_PATH` — key used to store weekly input data for predictions
- `MLFLOW_MODEL_URI` — (optional) MLflow URI if loading a model through MLflow

Set these locally e.g.:

```bash
export TOURISM_BUCKET=my-bucket-name
export FORECAST_CSV_PATH=data/predictions.csv
```


## Troubleshooting common errors

- "sklearn needs to be installed" in Lambda — solved by using `xgboost.Booster` directly to avoid scikit-learn dependency. See `src/lambda_package/forecast.py`.
- `ResourceNotFoundException` when updating Lambda — means the Lambda function does not exist in the account/region used by the workflow. Confirm `aws sts get-caller-identity` and `aws lambda get-function` and create the function if missing.
- Streamlit shows `Running load_data()` in spinner — this occurs when calling a `@st.cache_data` function inside a spinner. Use a session_state-based loader to suppress that status (see `src/dashboard/Weekly_Forecast.py`).


## Tests

There are no automated tests included yet. Recommended additions:
- Unit tests for preprocess & utils using `pytest`
- A smoke test for the Lambda predict path (load small model + data)


## Future features (ideas & roadmap)

- Improve Airflow DAG: break downstream steps into separate tasks with retries, alerts and monitoring
- Add integration tests for the Lambda image and local E2E test harness
- Shared caching for forecasts (Redis) for faster dashboard load across users
- Add a lightweight REST API (FastAPI) to serve forecasts and metadata
- Add user authentication on the dashboard and multi-tenant features
- Add end-to-end automated model promotion (staging -> production) using MLflow


## Contributing

Open issues and pull requests are welcome. If you'd like help integrating with a cloud account, consider adding automation secrets and a test account to the repo settings.


## Contact

If you need hands-on assistance with deployment or CI tweaks, tell me what environment you deploy to (AWS account/region) and I can prepare a PR to update workflows or add helper scripts.


---

Generated by an automated assistant — feel free to edit or extend.