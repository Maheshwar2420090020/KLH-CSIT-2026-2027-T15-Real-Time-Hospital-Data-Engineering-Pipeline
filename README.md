# Real-Time Hospital Data Engineering Pipeline

An end-to-end **Data Engineering and AI platform** for processing, validating, storing, and analyzing continuously generated streaming data in real time.

---

##  Overview

**Real-Time Hospital Data Engineering Pipeline** is a real-time streaming analytics platform designed to handle high-volume, continuously generated hospital data with minimal latency.

The system collects raw patient and hospital operational events from multiple sources such as **patient monitoring devices, electronic health records, laboratory systems, pharmacy systems, and hospital APIs**. These events are processed and transformed in real time, validated for data quality, stored using scalable storage technologies, and analyzed using machine learning for anomaly detection.

The platform combines **real-time hospital data engineering with AI-based analytics** to convert raw streaming data into clean, reliable, and actionable information.

---

##  Problem Statement

Modern hospitals continuously generate large amounts of data from APIs, IoT devices, application logs, and operational systems.

Traditional batch-processing systems can introduce significant delays, making it difficult to obtain timely insights. Streaming data can also contain:

* Missing values
* Duplicate records
* Schema inconsistencies
* Malformed inputs
* Unusual or anomalous events

Data is often distributed across different processing, storage, and analytics environments, making real-time monitoring difficult.

**Real-Time Hospital Data Engineering Pipeline addresses these challenges by providing an integrated pipeline for real-time data ingestion, processing, data-quality validation, anomaly detection, storage, API access, and monitoring.**

---

##  Objectives

* Build a low-latency real-time hospital data ingestion pipeline.
* Process continuously generated patient and hospital operational events.
* Perform automated data-quality validation.
* Detect anomalies and unusual patient and operational patterns.
* Store raw and processed hospital data efficiently.
* Provide APIs for accessing processed hospital data and ML results.
* Provide real-time visualization and monitoring.
* Build a scalable and reliable streaming architecture.

---

##  Proposed Solution

Real-Time Hospital Data Engineering Pipeline provides an end-to-end streaming data pipeline consisting of multiple stages.

### 1. Real-Time Data Ingestion

**Apache Kafka** collects continuous hospital events from multiple data sources and acts as the real-time ingestion layer.

### 2. Stream Processing

**Apache Spark Structured Streaming** and **PySpark** process incoming events in real time. The system performs transformations and aggregations while maintaining low processing latency.

### 3. Data Quality

**Great Expectations** validates incoming data by performing schema, consistency, and quality checks. This helps identify invalid and unreliable records before they are used for downstream analytics.

### 4. Hybrid Storage

Processed and raw datasets are stored using:

* **MinIO** – S3-compatible object storage
* **Apache Parquet** – Efficient analytical data storage
* **PostgreSQL** – Structured relational data persistence

### 5. Anomaly Detection

**Scikit-learn** is used to analyze processed streaming data and identify unusual patterns and outliers.

### 6. API Layer

**FastAPI** provides high-performance REST APIs for accessing processed hospital data and machine-learning services.

### 7. Monitoring and Visualization

**Grafana** provides real-time dashboards for monitoring pipeline performance and displaying detected anomalies.

---

##  System Workflow

```text
Hospital Hospital Data Sources
     │
     ▼
Apache Kafka
     │
     ▼
Spark Structured Streaming
     │
     ▼
Data Quality Validation
(Great Expectations)
     │
     ▼
Data Storage
 ┌───────────────┐
 │     MinIO     │
 │    Parquet    │
 │  PostgreSQL   │
 └───────────────┘
     │
     ▼
Anomaly Detection
   (Scikit-learn)
     │
     ▼
FastAPI
     │
     ▼
Grafana Dashboard
```

### Workflow Explanation

1. Hospital data is continuously generated from patient-monitoring devices, EHR systems, laboratory systems, pharmacy systems, and hospital applications.
2. Apache Kafka receives and streams these events.
3. Spark Structured Streaming consumes the events and performs real-time processing.
4. Great Expectations validates the quality and consistency of the processed hospital data.
5. Validated data is stored in MinIO, Parquet, and PostgreSQL.
6. Scikit-learn analyzes the processed hospital data and identifies anomalies.
7. FastAPI exposes processed hospital data and machine-learning results.
8. Grafana visualizes real-time system performance and detected anomalies.

###  Workflow Summary

**Collect → Process → Validate → Store → Detect → Serve → Monitor**

---

##  Key Features

*  Real-time event ingestion
*  Continuous stream processing
*  Automated data-quality validation
*  Data cleansing and transformation
*  Hybrid data storage
*  AI-based hospital anomaly detection
*  REST API access
*  Real-time monitoring dashboards
*  Scalable streaming architecture
*  Containerized deployment support

---

## Novelty

The main novelty of Real-Time Hospital Data Engineering Pipeline is its **end-to-end integration of multiple data engineering and AI capabilities into a single real-time hospital data platform**.

Instead of focusing only on stream processing, data quality, or anomaly detection, Real-Time Hospital Data Engineering Pipeline combines:

**Real-time hospital data ingestion + stream processing + data-quality validation + hybrid storage + anomaly detection + API serving + unified monitoring**

into one integrated pipeline.

The novelty is therefore primarily **architectural and integrative**, rather than introducing a completely new machine-learning algorithm.

---

## Technology Stack

| Category           | Technology                 | Purpose                             |
| ------------------ | -------------------------- | ----------------------------------- |
| Programming        | Python, PySpark            | Pipeline development and processing |
| Data Ingestion     | Apache Kafka               | Real-time event streaming           |
| Stream Processing  | Spark Structured Streaming | Real-time transformation            |
| Data Quality       | Great Expectations         | Validation and quality checks       |
| Data Lake          | MinIO                      | S3-compatible object storage        |
| Analytical Storage | Apache Parquet             | Efficient analytical storage        |
| Database           | PostgreSQL                 | Structured data persistence         |
| Machine Learning   | Scikit-learn               | Anomaly detection                   |
| API                | FastAPI                    | Data and ML services                |
| Monitoring         | Grafana                    | Real-time dashboards                |
| DevOps             | Docker                     | Containerization                    |
| Version Control    | Git                        | Source-code management              |

---

## Architecture

```text
                    ┌──────────────────────┐
                    │     Hospital Data Sources     │
                    │ Patient Devices | EHR | Lab    │
                    │ Pharmacy | APIs | Operations  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Apache Kafka      │
                    │  Real-Time Ingestion │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │ Spark Structured Streaming     │
              │          + PySpark             │
              │ Processing & Transformation    │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌──────────────────────┐
                    │  Great Expectations  │
                    │   Data Validation    │
                    └──────────┬───────────┘
                               │
                               ▼
             ┌──────────────────────────────────┐
             │          Hybrid Storage          │
             │                                  │
             │ MinIO | Apache Parquet | PostgreSQL │
             └────────────────┬─────────────────┘
                              │
                              ▼
                    ┌──────────────────────┐
                    │   Scikit-learn       │
                    │ Anomaly Detection    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │       FastAPI        │
                    │      REST APIs       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │       Grafana        │
                    │ Monitoring & Alerts  │
                    └──────────────────────┘
```

---

## Expected Outcomes

Real-Time Hospital Data Engineering Pipeline is designed to provide:

* Faster processing of continuously generated hospital data.
* Improved data reliability through automated validation.
* Early identification of unusual patient and operational events.
* Scalable storage for raw and processed hospital datasets.
* Centralized monitoring and visualization.
* Better availability of processed hospital data through APIs.
* Faster access to insights for hospital staff and administrators.

---

## Future Scope

The platform can be extended with:

* Automated alerting for critical anomalies.
* Advanced machine-learning models.
* Real-time prediction capabilities.
* Cloud-based deployment.
* Automated pipeline scheduling.
* More advanced analytics dashboards.
* Integration with additional EHR, laboratory, pharmacy, and IoT data sources.

---

##  Team

| Name          | Roll number |
| ------------- | ----------- |
| Y. Nihitha    | 2420090099  |
| S. Harini     | 2420030523  |
| A. Sri Anitha | 2420090117  |

### Project Guide

**Dr. N. Shirisha**
Associate Professor

---

##  Academic Information

**Course:** Fundamentals of Data Engineering
**Course Code:** 24DEA3101
**Academic Year:** 2026–27

---

##  Project Focus

**Real-Time Hospital Data Engineering Pipeline**

> *Transforming continuous raw patient and hospital operational events into clean, reliable, and actionable intelligence through real-time data engineering and AI.*
