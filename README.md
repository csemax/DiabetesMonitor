# 🏥 Real-Time Health Monitoring System (Lambda Architecture)

![Status](https://img.shields.io/badge/Status-Prototype-green?style=flat-square)
![Architecture](https://img.shields.io/badge/Architecture-Lambda_Architecture-blue?style=flat-square)
![Tech](https://img.shields.io/badge/Spark-Kafka-orange?style=flat-square)
![Cache](https://img.shields.io/badge/Redis-Stack-red?style=flat-square)

A scalable **IoT Big Data Healthcare System** designed to monitor Diabetes (Glucose) and Heart Rate in real-time. This project implements the **Lambda Architecture** to handle massive data streams with low latency while simultaneously archiving data for historical analysis.

---

## 🧠 System Architecture

The system follows the Lambda Architecture pattern, divided into three main layers: **Speed Layer** (Real-time), **Batch Layer** (Historical), and **Serving Layer** (Dashboard).

```mermaid
graph LR
    subgraph "1. Edge Layer"
    A[Sensor Simulator] -->|JSON Stream| B(Kafka Broker)
    end

    subgraph "2. Speed Layer (Real-time)"
    B -->|Stream| C{Spark Structured Streaming}
    C -->|Validate & Process| D[(Redis Cache)]
    end

    subgraph "3. Batch Layer (Historical)"
    B -->|Stream| E[Archiver Script]
    E -->|Write CSV| F[Data Lake / Storage]
    F -->|Read CSV| G{Spark Batch Job}
    G -->|Aggregated Stats| D
    end

    subgraph "4. Serving Layer"
    D -->|Fetch Data| H[Node.js Server]
    H -->|WebSocket| I[Web Dashboard]
    end
```
✨ Key Features
⚡ Real-Time Visualization: Live streaming graph (Last 60 seconds) for Glucose trends via WebSockets.

🛡️ Business Logic Validation: Automatically filters & drops invalid sensor data (e.g., negative heart rates, impossible glucose levels) to ensure data integrity before processing.

🚨 Smart Alerts: Real-time status classification (NORMAL, WARNING, CRITICAL_HIGH, CRITICAL_LOW).

💾 Data Lake Archiving: Stores raw sensor data into CSV files for audit trails and deep learning.

📊 Batch Analytics: On-demand calculation of daily statistics (Average, Min, Max, Total Records) from the Data Lake.

🧹 Auto-Initialization: Includes a script to clean the database and prepare the environment instantly.

---

⚙️ Prerequisites
Ensure you have the following installed on your machine:

Docker Desktop (Required for Kafka & Redis containers)

Python 3.8+

Node.js & npm

Java JDK 8 or 11 (Required for Apache Spark)

pip install kafka-python pyspark redis pandas numpy
