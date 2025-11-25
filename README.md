# Trentino Tourism Forecast – Weekly Tourism Demand Prediction

## 📌 Project Overview

This project predicts a weekly Tourism Experience Index for Trentino, Italy — an aggregated indicator derived from public mobility, weather, and accommodation data. 
The system automatically fetches data, preprocesses it, generates weekly forecasts, and updates a live dashboard.

## 📂 Repository Structure
```bash
├── src
│   ├── dashboard/                 # Streamlit or web dashboard
│   ├── etl/                       # All data preprocessing and cleaning scripts
│   ├── lambda_package/            # Dockerized Lambda for automated weekly forecasting
│   └── train_xgboost.py           # ML model training
├── utils/                         # Shared utilities
├── tests/                         # Pytest unit tests
└── README.md
```
## 🧠 How It Works
**1. Data Ingestion** 

* Collects weather, GTFS public transport, and tourism movement datasets.
* Cleans, aggregates, and aligns them by week-year.
* Stores intermediate data in cloud storage.

**2. Machine Learning Model**


* Uses an XGBoost-based predictor.

* Trains on features from mobility + weather + past tourism data.
* Produces next week’s predicted Tourism Experience Index.
* The model achieved 0.05 MAE on the test set, indicating high accuracy for weekly index forecasting.

**3. Weekly Forecast Automation (AWS Lambda + ECR)**

* A Dockerized AWS Lambda (running via ECR image) triggered every week.

* Runs the forecasting script (forecast.py) and writes results to storage.

**4. Dashboard**

* Containerised dashboard running on EC2 (Free Tier eligible).

* Fetches the latest predictions and visualises them.

## ☁️ AWS Architecture

The system uses a simplified cloud pipeline:

### Compute

**AWS Lambda (Docker-based)**
* Runs forecasting tasks weekly.

**Amazon EC2** 
* Hosts the tourism analytics dashboard.

### Orchestration

**EventBridge Trigger**
* Schedules weekly Lambda execution.

### Container Registry

**Amazon ECR**
* Stores the Lambda and dashboard image.

### Storage

**S3**
* Store aggregated datasets or predictions.


## 📊 Datasets Used

All datasets are publicly available and licensed under open data / Creative Commons terms.

**Accommodation & Tourism Arrivals (Provincia Autonoma di Trento)**

🔗 [https://statweb.provincia.tn.it/movturistico/index.asp](https://statweb.provincia.tn.it/movturistico/index.asp)

* Contains monthly tourism arrivals/presences.

**Public Transport Mobility – GTFS (Trentino Trasporti)**

🔗 [https://www.trentinotrasporti.it/it/opendata-it](https://www.trentinotrasporti.it/it/opendata-it)

* Bus routes, stops, and timetables.

**Weather Data – Open-Meteo API**

🔗 [https://open-meteo.com](https://open-meteo.com)

* Historical and forecasted meteorological parameters.

**All datasets are reused under their respective public licenses.**
- Attribution: Provincia Autonoma di Trento (CC-BY), Trentino Trasporti Open Data, Open-Meteo API.

## ✨ Features
### ✅ Current Features

- Automated weekly forecasting using AWS Lambda

- Containerised dashboard deployed on AWS EC2

- Clean separation between ETL, model, and dashboard

- Public datasets only — no private data required

- Portable via Docker and reproducible

### ⚠️ Limitations

Every real-world forecasting system has constraints—here are the main ones for this project:

**1. Long Feedback Loop**

Tourism data (arrivals/presences) is often released seasonally, causing lag in the ground truth collection.

**2. Mobility Proxy Limited to Public Transport**

GTFS data captures only bus usage, missing:

* Cars

* Bikes

* Trains

* Car rentals

* Camping mobility
This introduces bias.

**3. Model Generalisation Bound to Region**

* Currently centred on Trentino only, not yet suitable for national or EU-level forecasting.

**4. No Real-Time Drift Detection**

* If mobility/tourism habits change, the model may degrade without warning.

**5. Predictions available only for the next two weeks**
* The system currently forecasts only this week and next week.

### 🚀 Future Improvements
#### 🔧 Model & Data Enhancements

* Add event-based features (festivals, conferences, sports events, etc.)

* Integrate more mobility layers (bike-sharing, car traffic, train ridership)

#### 📉 Data Quality & Frequency

* Collaborate with local authorities to provide higher-frequency tourism data (weekly instead of monthly)

* Implement data drift detection periodically 

#### 🔄 Automatic Retraining

* Add a scheduled retraining pipeline (via AWS Step Functions + SageMaker or GitHub Actions)

### 🌍 Scalability

* Extend the project to:

- Whole Italy

- Alpine region

- European-wide tourism forecast network

### Extend Forecast Horizon
* Use a paid api for a longer period of forecasted data

## 📦 Deployment
**Dashboard:**

- Deployed via Docker on EC2.

**Forecast Lambda:**

- Build Docker image → push to ECR

- Create Lambda with --package-type Image

- Scheduled via EventBridge rule

---
## 🤝 Collaboration

This project is fully open-source, and contributions are welcome.
You can collaborate by:
* Enhancing the dashboard UI/UX
* Adding new datasets (events, mobility, transportation)
* Improving ETL pipelines
* Testing new ML models (LightGBM, Deep Learning, Temporal Fusion Transformers)
* Improving AWS deployment workflows
If you are a researcher, student, or local authority member interested in tourism analytics, feel free to open an issue or pull request!

