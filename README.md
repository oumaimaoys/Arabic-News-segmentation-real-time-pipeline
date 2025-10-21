# Arabic News Segmentation – Real-Time Pipeline

A simple, end-to-end pipeline that ingests **Arabic news headlines** in real time, segments them by **topic**, and makes the results queryable for analytics.

---

## Overview
- **Ingest** Arabic news from news websites and push events to **Kafka**.
- **Process** streams with **PySpark** to clean text and assign a **topic label**.
- **Store** curated results in a warehouse/table for easy querying.
- **Orchestrate** everything on a schedule with **Airflow**.
- **Visualize** topic trends over time in your BI tool (Superset/Power BI).

---

## Features
- Real-time headline ingestion (Kafka topic: `news_headlines`)
- Language-aware text cleaning (Arabic stopwords/normalization)
- Fast topic segmentation (e.g., TF-IDF + clustering or a lightweight classifier)
- Incremental writes (Parquet/Delta or warehouse tables)
- Simple, reproducible local setup with Docker

---

## Tech Stack
- **Streaming**: Apache Kafka
- **Processing**: PySpark (Structured Streaming)
- **Orchestration**: Apache Airflow
- **Storage**: Parquet on local/S3 (or a warehouse table)
- **Python**: requests, pandas, pyspark
- **(Optional)**: Plotly

---

## Quick Start

1) **Clone**
```bash
git clone https://github.com/oumaimaoys/Arabic-News-segmentation-real-time-pipeline.git
cd Arabic-News-segmentation-real-time-pipeline
```

2) **Create `.env`** (do **not** commit secrets)
```env
KAFKA_BOOTSTRAP=localhost:9092
S3_BUCKET=your-bucket-name         
OUTPUT_PATH=data/curated            # local fallback path
```

3) **Start Kafka (Docker)**
```bash
docker compose up -d
# or: docker-compose up -d
```

4) **Run the News Producer**
```bash
pip install -r requirements.txt
python producers/news_producer.py   # publishes Arabic headlines to Kafka
```

5) **Start the Stream Processor**
```bash
python spark/stream_segment.py      # Kafka -> PySpark -> segmented output
```

6) **Schedule with Airflow (optional)**
```bash
airflow db init
airflow users create --username admin --password admin --role Admin --email you@example.com --firstname You --lastname Dev
airflow webserver -p 8080 & airflow scheduler
# Enable DAG: news_segmentation_dag
```

---


