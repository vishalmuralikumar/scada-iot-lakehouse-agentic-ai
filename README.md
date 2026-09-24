# SCADA IoT Lakehouse & Agentic AI Platform

An end-to-end **Data Engineering, Lakehouse, Machine Learning, Analytics, and Agentic AI platform** built using **AWS and Databricks**.

The project processes industrial SCADA sensor data using a modern Medallion Architecture, performs automated data-quality validation and anomaly detection, exposes analytical insights through Databricks SQL and AI/BI dashboards, and enables conversational access to governed data through **Databricks Genie, Supervisor Agents, and a custom Model Context Protocol (MCP) server**.

---

## Project Overview

Industrial systems continuously generate large volumes of telemetry from sensors measuring vibration, acceleration, velocity, temperature, pressure, flow, displacement, and energy consumption.

Raw sensor data by itself provides limited business value.

Organizations need a platform capable of:

- ingesting sensor data
- processing continuously arriving files
- validating data quality
- separating invalid records
- creating trusted analytical datasets
- detecting abnormal sensor behavior
- tracking machine-learning models
- visualizing operational metrics
- allowing users to query data using natural language
- enabling AI agents to securely interact with enterprise data

This project demonstrates how these capabilities can be combined into a single **modern Databricks Lakehouse and Agentic AI architecture**.

---

# Project Purpose

The primary purpose of this project is to demonstrate how a modern industrial IoT data platform can transform raw SCADA sensor measurements into trusted analytical and AI-ready information.

The solution combines:

```text
Cloud Storage
      +
Data Engineering
      +
Streaming Processing
      +
Lakehouse Architecture
      +
Data Quality
      +
Governance
      +
Analytics
      +
Machine Learning
      +
MLOps
      +
Generative AI
      +
Agentic AI
      +
MCP
```

The project is designed as both a **real-world engineering implementation and a learning platform for advanced Databricks concepts**.

---

# Architecture

```text
                    INDUSTRIAL SCADA DATA
                             |
                             v
                    Historical Dataset
                             |
                             v
                         AWS S3
                 Historical Data Storage
                             |
                             v
                     Databricks Platform
                             |
                             v
               Databricks Volume / File Replay
                             |
                             v
                        Auto Loader
                             |
                             v
                Spark Structured Streaming
                             |
                             v
                  MEDALLION ARCHITECTURE
                             |
              +--------------+--------------+
              |                             |
              v                             |
          BRONZE LAYER                      |
        Raw Sensor Records                  |
              |                             |
              v                             |
          SILVER LAYER                      |
      Clean + Validated Records             |
              |                             |
        Data Quality Rules                  |
              |                             |
       +------+-------+                     |
       |              |                     |
       v              v                     |
  Valid Records   Quarantine Records        |
       |                                    |
       v                                    |
           GOLD ANALYTICS                   |
      Aggregated Sensor Metrics             |
              |                             |
              +-----------------------------+
              |
              v
       Databricks SQL Warehouse
              |
      +-------+-----------------------+
      |                               |
      v                               v
 AI/BI Dashboard                 ML Pipeline
                                      |
                                      v
                             Feature Engineering
                                      |
                     +----------------+----------------+
                     |                                 |
                     v                                 v
              Rolling Z-Score                  Isolation Forest
                     |                                 |
                     +----------------+----------------+
                                      |
                                      v
                              Anomaly Results
                                      |
                                      v
                                    MLflow
                                      |
                         Experiment Tracking
                         Model Registration
                         Model Versioning
                         Champion Alias
                                      |
                                      v
                               Genie Analytics
                                      |
                                      v
                             Supervisor Agent
                                      |
                +---------------------+--------------------+
                |                                          |
                v                                          v
           Python Tool                            Custom MCP Server
                                                           |
                                                           v
                                                    Databricks App
                                                           |
                                                           v
                                                    SQL Warehouse
                                                           |
                                                           v
                                                  Gold SCADA Tables
```

---

# Dataset

The project uses the:

**I-BiDaaS CRF SCADA Dataset**

The dataset contains industrial sensor measurements collected from automotive welding and manufacturing equipment.

The SCADA dataset contains measurements from approximately **147 sensors** across multiple sensor categories.

Sensor types include:

- acceleration
- velocity
- temperature
- pressure
- flow
- displacement
- water consumption
- air consumption
- industrial energy measurements

The raw sensor data follows a structure similar to:

```text
Id
Value
Unit
Timestamp
```

The historical source archive contains approximately:

```text
869 files
31 GB uncompressed data
```

The historical dataset was stored in **Amazon S3** as the cloud landing/archive layer.

For the implemented Databricks pipeline, representative SCADA data was replayed incrementally through Databricks Volumes to demonstrate file-arrival automation, Auto Loader, Structured Streaming, and Lakeflow processing.

---

# Data Engineering Architecture

The project follows the **Medallion Architecture**.

```text
Raw Data
   |
   v
Bronze
   |
   v
Silver
   |
   v
Gold
```

Each layer has a specific responsibility.

---

## Bronze Layer

The Bronze layer stores raw sensor records with minimal transformation.

Its primary purpose is to preserve incoming records before business rules and data-quality transformations are applied.

Typical fields include:

```text
Sensor ID
Sensor Value
Measurement Unit
Timestamp
```

The Bronze layer acts as the raw ingestion layer of the Lakehouse.

---

## Silver Layer

The Silver layer contains cleaned and validated sensor data.

Lakeflow expectations are used to verify important fields.

Examples include:

```text
Sensor ID IS NOT NULL
Sensor Value IS NOT NULL
Measurement Unit IS NOT NULL
Timestamp IS NOT NULL
```

Invalid records are prevented from entering the trusted Silver dataset.

Instead of simply losing invalid records, the project also preserves problematic records in a separate **quarantine dataset**.

Architecture:

```text
Bronze
   |
   +----------------------+
   |                      |
   v                      v
Valid Records         Invalid Records
   |                      |
   v                      v
Silver              Quarantine Table
```

This is useful because production data pipelines should preserve bad records for debugging, investigation, and remediation.

---

# Gold Layer

The Gold layer contains analytical sensor summaries.

The primary Gold table is:

```text
workspace.gold.scada_lakeflow_gold
```

Example Gold metrics include:

```text
Sensor ID
Measurement Unit
Reading Count
Average Value
Minimum Value
Maximum Value
```

The Gold layer provides business-ready information to:

- Databricks SQL
- AI/BI dashboards
- machine-learning workflows
- Genie
- Supervisor Agent
- MCP tools

---

# Lakeflow Declarative Pipeline

The project uses **Databricks Lakeflow Declarative Pipelines** to manage the Medallion transformations.

Pipeline flow:

```text
Auto Loader
     |
     v
Bronze Streaming Table
     |
     +---------------> Quarantine
     |
     v
Silver Streaming Table
     |
     v
Gold Analytics
```

The pipeline demonstrates:

- declarative transformations
- streaming tables
- materialized analytical results
- expectations
- data-quality validation
- quarantine architecture
- lineage between pipeline layers

---

# Data Quality

A key requirement of the project is preventing invalid sensor records from contaminating trusted analytical data.

Lakeflow Expectations are used to validate incoming records.

The project demonstrates both:

```text
ALLOW
```

and:

```text
DROP
```

data-quality behavior.

The final trusted Silver pipeline drops records that violate required-field expectations while the quarantine path preserves invalid records separately.

This provides visibility into data-quality problems without allowing invalid records into trusted analytics.

---

# Incremental Ingestion

The project uses **Databricks Auto Loader** for incremental file processing.

Auto Loader continuously detects newly arriving files and processes only data that has not already been consumed.

This avoids repeatedly scanning and processing the entire source dataset.

Architecture:

```text
Incoming Files
      |
      v
Databricks Auto Loader
      |
      v
Incremental Processing
      |
      v
Bronze Streaming Table
```

---

# File Arrival Automation

A Databricks Job was configured with a **File Arrival Trigger**.

When a new SCADA file arrives in the monitored location:

```text
New Sensor File
      |
      v
File Arrival Trigger
      |
      v
Databricks Job
      |
      v
Streaming Pipeline
      |
      v
Bronze
      |
      v
Silver
      |
      v
Gold
```

This removes the need to manually start the pipeline each time new data becomes available.

---

# Apache Spark

Apache Spark is used as the distributed processing engine.

The project demonstrates:

```text
Spark DataFrames
Spark SQL
Structured Streaming
Window Functions
Aggregations
Transformations
Incremental Processing
Feature Engineering
```

The project also exposed practical Spark optimization concepts including partitioning and the effect of unpartitioned Window operations.

---

# Delta Lake

Processed data is stored using **Delta Lake**.

Delta Lake provides capabilities such as:

- ACID transactions
- reliable table updates
- schema management
- streaming and batch interoperability
- scalable Lakehouse storage

Delta tables form the storage foundation of the Bronze, Silver, and Gold architecture.

---

# Unity Catalog

**Unity Catalog** is used to organize and govern project data.

The project uses schemas including:

```text
workspace.bronze
workspace.silver
workspace.gold
workspace.ml
```

Unity Catalog provides a centralized governance layer for:

```text
Tables
Schemas
Models
Permissions
Lineage
Metadata
```

It is also used for ML model registration.

---

# Databricks SQL

Databricks SQL is used to query the Gold analytical layer.

Example analytical metrics include:

```text
Total Sensors
Total Readings
Average Sensor Value
Peak Sensor Value
Minimum Value
Maximum Value
Anomaly Counts
```

One project query produced:

```text
Total Sensors: 32
Total Readings: 282,866
Overall Average Value: 311.07
Peak Value: 35,026.04
```

---

# AI/BI Dashboard

The project includes an:

**SCADA IoT Monitoring Dashboard**

The dashboard uses Gold-layer data and Databricks SQL to visualize operational metrics.

Examples include:

```text
Total Sensors
Total Sensor Readings
Sensor Statistics
Average Sensor Values
Minimum Values
Maximum Values
Anomaly Information
```

This demonstrates how a Lakehouse engineering pipeline can directly support analytical and operational reporting.

---

# Machine Learning

The project extends beyond Data Engineering by adding an anomaly-detection workflow.

Sensor data from the Silver layer is transformed into machine-learning features.

---

# Feature Engineering

The following features were generated:

```text
value

rolling_avg_10

rolling_std_10

z_score

value_change

rolling_min_10

rolling_max_10

hour

day_of_week
```

These features allow the system to understand both the current sensor reading and its behavior relative to recent observations.

---

# Z-Score Anomaly Detection

A rolling Z-score approach is used as the statistical anomaly-detection baseline.

The method measures how far a sensor value deviates from its recent rolling behavior.

The project identified approximately:

```text
18,741
```

Z-score anomaly candidates.

This statistical method provides an interpretable baseline that can be compared against machine-learning results.

---

# Isolation Forest

The project uses **Isolation Forest** as the machine-learning anomaly-detection model.

Isolation Forest is useful for unsupervised anomaly detection because the dataset does not contain verified equipment-failure labels.

Training configuration:

```text
Training Rows: 100,000

ML Features: 8
```

Training results:

```text
Training Anomalies: 10,597

Training Anomaly Rate: ~10.6%
```

The trained model was then applied to the complete engineered dataset.

Full-dataset results:

```text
Rows Evaluated: 282,802

ML Anomalies: 26,962

ML Anomaly Rate: ~9.53%
```

---

# Statistical + ML Anomaly Comparison

The project combines two anomaly-detection techniques:

```text
Rolling Z-Score
+
Isolation Forest
```

The results are categorized into four groups.

| Category | Meaning |
|---|---|
| NORMAL | Neither method detected an anomaly |
| ML_ONLY | Isolation Forest detected the anomaly |
| Z_SCORE_ONLY | Rolling Z-score detected the anomaly |
| HIGH_CONFIDENCE | Both techniques detected the anomaly |

Observed results:

```text
NORMAL
241,098

ML_ONLY
22,963

Z_SCORE_ONLY
14,742

HIGH_CONFIDENCE
3,999
```

`HIGH_CONFIDENCE` represents observations where both methods agree that the reading is unusual.

It does **not** represent a confirmed equipment failure because the original SCADA dataset does not provide verified failure labels.

---

# Gold Anomaly Table

Final anomaly-detection results are stored in:

```text
workspace.gold.scada_anomaly_results
```

The table includes fields such as:

```text
id

timestamp

unit

value

value_change

rolling_avg_10

rolling_std_10

rolling_min_10

rolling_max_10

hour

day_of_week

z_score

zscore_is_anomaly

ml_anomaly_score

ml_is_anomaly

anomaly_category
```

This Gold dataset can be consumed by analytics, dashboards, Genie, and AI agents.

---

# MLflow

**MLflow** is used to manage the machine-learning lifecycle.

The project demonstrates:

```text
Experiment Tracking

Model Logging

Model Signature

Model Registration

Model Versioning

Model Loading

Model Aliases
```

The Isolation Forest model is registered in Unity Catalog as:

```text
workspace.ml.scada_isolation_forest
```

Model version:

```text
Version 1
```

The project also uses the alias:

```text
champion
```

This allows applications to reference:

```text
workspace.ml.scada_isolation_forest@champion
```

instead of hard-coding a specific model version.

---

# Databricks Genie

The project includes a Databricks Genie Agent named:

```text
SCADA Sensor Analytics
```

Genie connects to:

```text
workspace.gold.scada_lakeflow_gold
```

and:

```text
workspace.gold.scada_anomaly_results
```

Users can ask natural-language questions such as:

```text
Which sensor has the most HIGH_CONFIDENCE anomalies?

How many HIGH_CONFIDENCE anomalies were detected?

Which sensors have the highest average readings?

Show anomaly counts by sensor.

Compare ML anomalies and Z-score anomalies.

What is the average value for a particular sensor?
```

Genie converts natural-language questions into analytical queries over governed Lakehouse data.

---

# Supervisor Agent

The project includes a Supervisor Agent named:

```text
scada-iot-supervisor-agent
```

The Supervisor Agent coordinates multiple tools.

Architecture:

```text
User
  |
  v
Supervisor Agent
  |
  +------------------------+
  |                        |
  v                        v
Genie Agent           Python Tool
  |
  v
SCADA Analytics
```

The Supervisor Agent determines which tool should be used depending on the user's question.

This provides an **Agentic AI orchestration layer** on top of the Lakehouse.

---

# Model Context Protocol

The project also implements a custom **Model Context Protocol (MCP) server**.

MCP allows AI agents to interact with external tools and enterprise systems using a standardized tool interface.

The custom MCP application is deployed using:

```text
Databricks Apps
```

Application name:

```text
mcp-scada-iot
```

---

# MCP Architecture

```text
User
  |
  v
Supervisor Agent
  |
  v
mcp-scada-iot
  |
  v
Custom MCP Server
  |
  v
Databricks App
  |
  v
SQL Warehouse
  |
  v
Unity Catalog
  |
  v
Gold SCADA Tables
```

The MCP application is connected to a Databricks SQL Warehouse.

This allows custom MCP tools to execute governed analytical queries.

---

# Custom MCP Tool

One implemented MCP function is:

```python
get_sensor_summary(sensor_id)
```

The tool queries:

```text
workspace.gold.scada_lakeflow_gold
```

It returns:

```text
Sensor ID
Measurement Unit
Reading Count
Average Value
Minimum Value
Maximum Value
```

Example request:

```text
Use the MCP server to get the summary for sensor 64.
```

Example result:

```text
Sensor ID: 64

Measurement Unit: mg

Total Readings: 24,751

Average Value: 523.06

Minimum Value: 24.49

Maximum Value: 6,649.67
```

This verifies the complete integration:

```text
Supervisor Agent
      |
      v
MCP Server
      |
      v
Databricks App
      |
      v
SQL Warehouse
      |
      v
Gold Lakehouse Data
```

---

# Technology Stack

| Category | Technology |
|---|---|
| Cloud | AWS |
| Historical Storage | Amazon S3 |
| Data Platform | Databricks |
| Distributed Processing | Apache Spark |
| Streaming | Spark Structured Streaming |
| Incremental Ingestion | Databricks Auto Loader |
| Storage Format | Delta Lake |
| Architecture | Medallion Architecture |
| Pipeline Framework | Lakeflow Declarative Pipelines |
| Governance | Unity Catalog |
| Data Quality | Lakeflow Expectations |
| Invalid Data Handling | Quarantine Architecture |
| Orchestration | Databricks Jobs |
| Trigger | File Arrival Trigger |
| Analytics | Databricks SQL |
| Visualization | Databricks AI/BI Dashboard |
| Feature Engineering | PySpark |
| Statistical Detection | Rolling Z-Score |
| Machine Learning | Isolation Forest |
| ML Lifecycle | MLflow |
| Model Registry | Unity Catalog Model Registry |
| Conversational Analytics | Databricks Genie |
| Agentic AI | Supervisor Agent |
| Agent Integration | Model Context Protocol |
| MCP Hosting | Databricks Apps |
| Version Control | Git |
| Repository | GitHub |
| Programming Language | Python |
| Query Language | SQL |

---

# End-to-End Data Flow

```text
SCADA Dataset
      |
      v
AWS S3 Historical Storage
      |
      v
Databricks
      |
      v
Incremental File Replay
      |
      v
Auto Loader
      |
      v
Structured Streaming
      |
      v
Bronze
      |
      v
Data Quality
      |
      +------------------+
      |                  |
      v                  v
Silver              Quarantine
      |
      v
Gold
      |
      +----------------------------+
      |                            |
      v                            v
Databricks SQL                ML Pipeline
      |                            |
      v                            v
AI/BI Dashboard            Feature Engineering
                                   |
                          +--------+--------+
                          |                 |
                          v                 v
                       Z-Score       Isolation Forest
                          |                 |
                          +--------+--------+
                                   |
                                   v
                             Anomaly Results
                                   |
                                   v
                                 MLflow
                                   |
                                   v
                            Model Registry
                                   |
                                   v
                                 Genie
                                   |
                                   v
                          Supervisor Agent
                                   |
                                   v
                             MCP Server
                                   |
                                   v
                            Databricks App
                                   |
                                   v
                            SQL Warehouse
                                   |
                                   v
                          Gold SCADA Analytics
```

---

# Project Use Cases

## Industrial Equipment Monitoring

The architecture can be used to monitor sensor readings generated by industrial machinery.

Engineers can inspect:

```text
sensor behavior
reading trends
statistical anomalies
ML anomalies
extreme measurements
```

---

## Manufacturing Analytics

Manufacturing companies can use similar pipelines to analyze data generated by:

```text
robots
welding equipment
motors
pumps
compressors
production lines
industrial machines
```

---

## Anomaly Detection

Combining statistical and machine-learning techniques provides multiple perspectives on abnormal sensor behavior.

The system identifies:

```text
statistical anomalies

machine-learning anomalies

high-confidence anomaly candidates
```

---

## Predictive Maintenance Foundation

This project can act as the foundation for a predictive-maintenance system.

With verified equipment failure and maintenance-history labels, future models could estimate:

```text
failure risk

remaining useful life

maintenance priority

equipment degradation
```

The current project performs anomaly detection and does not claim confirmed failure prediction.

---

## Operational Intelligence

Operational teams can use the Gold layer and dashboard to understand:

```text
sensor activity

reading volumes

measurement ranges

abnormal observations

high-risk anomaly candidates
```

---

## Conversational Analytics

Databricks Genie allows users to analyze operational information without manually writing SQL.

For example:

```text
Which sensor has the most anomalies?

Which sensor has the highest average reading?

How many high-confidence anomalies exist?

Show sensors with abnormal behavior.
```

---

## Agentic AI

The Supervisor Agent allows AI to decide which tool should handle the user's request.

This demonstrates how modern AI systems can orchestrate:

```text
Genie

Python

MCP

Databricks SQL

Governed Lakehouse Data
```

---

## Enterprise MCP Integration

The custom MCP server demonstrates how governed Databricks data can be exposed to AI agents through controlled tools rather than giving an AI application unrestricted database access.

---

# Why This Project Is Important

Traditional data engineering projects often stop after:

```text
Ingestion
   |
Transformation
   |
Dashboard
```

This project continues beyond traditional analytics:

```text
Data Engineering
        |
        v
Lakehouse
        |
        v
Governance
        |
        v
Analytics
        |
        v
Machine Learning
        |
        v
MLOps
        |
        v
Conversational AI
        |
        v
Agentic AI
        |
        v
MCP
```

The project demonstrates how a single modern data platform can support engineering, analytics, machine learning, and AI applications.

---

# Key Concepts Demonstrated

The project provides hands-on experience with:

- AWS cloud storage
- industrial IoT data
- SCADA data
- Apache Spark
- PySpark
- Spark DataFrames
- Spark SQL
- Spark Structured Streaming
- Auto Loader
- incremental ingestion
- Delta Lake
- Lakehouse Architecture
- Medallion Architecture
- Bronze layer
- Silver layer
- Gold layer
- data quality
- Lakeflow Expectations
- quarantine patterns
- Lakeflow Declarative Pipelines
- Databricks Jobs
- file-arrival automation
- Unity Catalog
- governance
- feature engineering
- Window functions
- rolling statistics
- statistical anomaly detection
- Isolation Forest
- unsupervised machine learning
- MLflow
- experiment tracking
- model signatures
- model registration
- model versioning
- model aliases
- Databricks SQL
- SQL Warehouse
- AI/BI Dashboards
- Databricks Genie
- Supervisor Agents
- Agentic AI
- Model Context Protocol
- custom MCP tools
- Databricks Apps
- Git
- GitHub

---

# Repository Structure

```text
scada-iot-lakehouse-agentic-ai/
│
├── apps/
│   │
│   └── mcp-scada-iot/
│       │
│       ├── server/
│       │   ├── app.py
│       │   ├── main.py
│       │   ├── tools.py
│       │   └── utils.py
│       │
│       ├── app.yaml
│       └── project configuration files
│
├── data visualization monitor/
│
├── scada_lakeflow_pipeline/
│   │
│   └── transformations/
│       └── my_transformation.py
│
├── scada_streaming_pipeline_job.ipynb
│
├── scada_ml_feature_engineering.ipynb
│
├── schema Batching and streaming.ipynb
│
├── README.md
│
└── LICENSE
```

---

# Main Databricks Tables

## Bronze

```text
workspace.bronze.scada_lakeflow_bronze
```

Raw streaming sensor records.

---

## Silver

```text
workspace.silver.scada_lakeflow_silver
```

Validated SCADA records.

---

## Quarantine

```text
scada_lakeflow_quarantine
```

Records that violate data-quality expectations.

---

## Gold Sensor Analytics

```text
workspace.gold.scada_lakeflow_gold
```

Sensor-level aggregated statistics.

---

## Gold Anomaly Results

```text
workspace.gold.scada_anomaly_results
```

Detailed statistical and ML anomaly-detection results.

---

# ML Model

Registered model:

```text
workspace.ml.scada_isolation_forest
```

Current model version:

```text
Version 1
```

Model alias:

```text
champion
```

The alias allows downstream applications to reference the approved model without depending on a hard-coded version.

---

# Example Sensor Analysis

Using the custom MCP tool, Sensor 64 returned:

```text
Sensor ID
64

Unit
mg

Total Readings
24,751

Average Value
523.06

Minimum Value
24.49

Maximum Value
6,649.67
```

This query traveled through:

```text
User
  |
  v
Supervisor Agent
  |
  v
MCP Server
  |
  v
Databricks App
  |
  v
SQL Warehouse
  |
  v
Gold Delta Table
```

---

# Challenges Solved

This project included several realistic engineering challenges.

### Large Dataset Management

The original SCADA archive was approximately 31 GB uncompressed.

Historical data was transferred and verified in Amazon S3.

---

### Data Quality

Incoming records can contain invalid or missing fields.

Lakeflow expectations and a quarantine architecture were used to prevent invalid records from entering trusted analytical datasets.

---

### Incremental Processing

Processing every historical file whenever new data arrives is inefficient.

Auto Loader provides incremental file discovery and processing.

---

### Pipeline Automation

Instead of manually starting the pipeline when new data arrives, a Databricks file-arrival trigger automatically starts processing.

---

### Unlabeled Sensor Data

The SCADA dataset does not contain verified machine-failure labels.

Therefore, the project uses unsupervised anomaly detection rather than falsely treating anomalies as confirmed equipment failures.

---

### Multiple Anomaly Techniques

Statistical and ML techniques can identify different types of unusual behavior.

The project compares Z-score and Isolation Forest results and creates a HIGH_CONFIDENCE category when both methods agree.

---

### Model Lifecycle Management

Machine-learning models should not exist only inside notebooks.

MLflow and Unity Catalog Model Registry are used to track, version, and manage the model.

---

### Natural-Language Analytics

Not every business user knows SQL.

Databricks Genie allows users to query Gold datasets using natural language.

---

### AI Tool Integration

The custom MCP server allows the Supervisor Agent to securely invoke custom analytical functions connected to Databricks data.

---

# Current Implementation Scope

The implemented project includes:

```text
AWS S3 historical storage

Databricks Lakehouse

Auto Loader

Structured Streaming

Bronze / Silver / Gold

Lakeflow

Data Quality

Quarantine

File Arrival Trigger

Databricks SQL

AI/BI Dashboard

Feature Engineering

Z-Score Anomaly Detection

Isolation Forest

MLflow

Model Registry

Genie

Supervisor Agent

Custom MCP Server

Databricks Apps

SQL-backed MCP Tool

GitHub Integration
```

---

# Future Enhancements

The platform can be extended further with:

```text
Amazon Kinesis live-stream ingestion

Real industrial IoT devices

Continuous model retraining

Model serving endpoints

Predictive maintenance using labeled failures

Remaining Useful Life prediction

Advanced Delta optimization

Liquid Clustering

Production monitoring

Data drift detection

Model drift detection

CI/CD pipelines

GitHub Actions

Infrastructure as Code

Terraform

Automated testing

Alerting

Additional MCP tools

External AI application

Real-time notifications
```

Amazon Kinesis is intentionally listed as a future enhancement because the current implemented project demonstrates incremental/streaming processing through Databricks Auto Loader and file-arrival automation rather than a completed production Kinesis integration.

---

# Future MCP Tools

The custom MCP server can be expanded with additional functions such as:

```python
get_sensor_summary(sensor_id)

get_anomaly_summary(sensor_id)

get_high_confidence_anomalies()

get_sensor_health(sensor_id)

compare_sensors(sensor_ids)

get_recent_anomalies()
```

This would allow the Supervisor Agent to perform increasingly advanced SCADA analytics through controlled tools.

---

# Skills Demonstrated

```text
Data Engineering

Cloud Data Engineering

Databricks Engineering

Lakehouse Engineering

Apache Spark

Structured Streaming

Data Quality Engineering

Data Governance

Data Analytics

Machine Learning

MLOps

IoT Analytics

SCADA Analytics

Generative AI

Agentic AI

Model Context Protocol

Git / GitHub
```

---

# Business Value

This architecture demonstrates how organizations can transform raw industrial sensor data into operational intelligence.

Potential benefits include:

```text
Earlier identification of unusual machine behavior

Reduced manual sensor-data analysis

Centralized governed data

Automated data processing

Improved data quality

Self-service analytics

Natural-language data exploration

Reusable ML models

AI-assisted operational analytics

Controlled AI access to enterprise data
```

---

# Project Highlights

```text
31 GB+ SCADA historical dataset

869 source files

AWS S3 cloud storage

Spark Structured Streaming

Databricks Auto Loader

Bronze / Silver / Gold architecture

Lakeflow data pipelines

Automated data-quality validation

Quarantine architecture

Databricks Jobs

File-arrival automation

Databricks SQL analytics

AI/BI dashboard

282K+ processed sensor readings

Statistical anomaly detection

Isolation Forest ML model

MLflow model lifecycle

Unity Catalog Model Registry

Databricks Genie

Supervisor Agent

Custom MCP server

Databricks Apps deployment

SQL Warehouse integration

GitHub version control
```

---

# Learning Outcomes

Through this project I gained hands-on experience designing an end-to-end modern Data and AI platform.

The project helped strengthen my understanding of:

```text
Distributed data processing

Lakehouse architecture

Streaming pipelines

Incremental ingestion

Data-quality engineering

Data governance

Delta Lake

Spark optimization concepts

ML feature engineering

Unsupervised anomaly detection

MLOps

Conversational analytics

AI agents

Tool orchestration

MCP

Production-style AI/data integration
```

---

# Architecture Summary

```text
SCADA Sensors
      |
      v
Historical Data
      |
      v
AWS S3
      |
      v
Databricks
      |
      v
Auto Loader
      |
      v
Structured Streaming
      |
      v
Bronze
      |
      v
Silver ---------> Quarantine
      |
      v
Gold
      |
      +---------------------+
      |                     |
      v                     v
Databricks SQL        Machine Learning
      |                     |
      v                     v
AI/BI Dashboard         MLflow
                            |
                            v
                      Model Registry
                            |
                            v
                          Genie
                            |
                            v
                    Supervisor Agent
                            |
                            v
                     Custom MCP Server
                            |
                            v
                     Databricks App
                            |
                            v
                     SQL Warehouse
                            |
                            v
                    Gold SCADA Tables
```

---

# Author

## Vishal

**Data Engineering | Databricks | Lakehouse | Cloud | Machine Learning | Agentic AI**

Built as an end-to-end portfolio project to demonstrate modern Data Engineering, Databricks Lakehouse Engineering, Machine Learning, MLOps, and Agentic AI concepts.

---

# License

This project is licensed under the **MIT License**.

See the `LICENSE` file for additional information.

---

# Final Project Summary

The **SCADA IoT Lakehouse & Agentic AI Platform** demonstrates how industrial sensor data can move through an entire modern Data and AI lifecycle:

```text
Raw Data
   |
   v
Cloud Storage
   |
   v
Data Ingestion
   |
   v
Streaming Processing
   |
   v
Lakehouse
   |
   v
Data Quality
   |
   v
Governance
   |
   v
Analytics
   |
   v
Machine Learning
   |
   v
MLOps
   |
   v
Conversational Analytics
   |
   v
Agentic AI
   |
   v
MCP
```

The project combines **AWS, Databricks, Apache Spark, Structured Streaming, Delta Lake, Lakeflow, Unity Catalog, Databricks SQL, AI/BI Dashboards, MLflow, Isolation Forest, Genie, Supervisor Agents, Model Context Protocol, Databricks Apps, Git, and GitHub** into a single end-to-end architecture.

---

**Author: Vishal**
