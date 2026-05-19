# dwh-socio-economic-etl

Automated Web Scraping-Based Data Warehouse System for X and Reddit about Socio-Economic Discourse in Indonesia 

This repository contains the final project for the Bachelor of Data Science program at the Faculty of Mathematics and Natural Sciences, Universitas Negeri Surabaya, Class 2024INT.

### Team Members

| Role | Name | Student ID |
| --- | --- | --- |
| Leader | Ketut Shridhara | 24031554115 |
| Member 1 | Illona Anindya | 24031554222 |
| Member 2 | Nazril Ravi Pratama | 24031554129 |

### Project Overview

Social media platforms like X and Reddit are primary spaces for public opinion on economic conditions in Indonesia. Discussions regarding fuel price hikes, minimum wage adjustments, exchange rate fluctuations, and mass layoffs generate massive daily data volumes, yet this data largely goes unanalyzed in any structured or systematic way. This project bridges the urgency gap between data availability and analytical accessibility by providing a tool to continuously monitor and aggregate public sentiment on socio-economic issues at scale.

We built a full ETL pipeline backed by a star schema in PostgreSQL, with automated scheduling via Apache Airflow and interactive OLAP exploration through Atoti.

### System Architecture and ETL Pipeline

The core challenge addressed by this pipeline is integrating unstructured, heterogeneous, and multilingual data from two structurally different platforms into a unified, queryable format.

**1. Extraction**
Two Python scraping scripts run in parallel, scheduled daily by Apache Airflow. The first uses a Nitter-based scraper for X, targeting keywords such as inflasi, BBM, UMR, rupiah, harga pangan, and PHK. The second extracts data from Reddit via the api.pullpush.io endpoint for subreddits including r/indonesia, r/economy, and r/investasi.

**2. Transformation**
The transformation phase integrates the different JSON structures from X and Reddit into a unified DataFrame. This step includes text normalization to remove semantic noise, sentiment analysis using RoBERTa for English content and IndoBERT for Indonesian content, LDA-based topic modeling to identify latent topics, and timestamp standardization to support the time granularity aspect of the Data Warehouse.

**3. Loading**
Data loading into the Data Warehouse is executed in two sequential stages orchestrated by Airflow. First, data is stored into staging tables in Supabase as a clean archive before further processing. Next, it is processed into a star schema containing a central fact table (fact_post) and four dimension tables (dim_time, dim_platform, dim_topic, dim_sentiment). After the loading process is completed, materialized views are refreshed to update the daily aggregations used for OLAP queries.

### Target Insights

Through the Data Presentation layer (Atoti OLAP cube), this pipeline targets four main insight directions:

1. Temporal sentiment tracking to see how average sentiment on a given topic shifts month to month.


2. Platform comparison to contrast sentiment distributions between X and Reddit.


3. Topic volume distribution to rank the most discussed socio-economic issues.


4. Engagement correlation analysis to test whether negatively toned posts generate higher engagement than neutral or positive ones.



---

### Deployment Instructions

The following setup steps rely on your system environment. This is expected behavior for deploying the pipeline, but success is not guaranteed without the proper configuration of Python, Supabase, and Apache Airflow.

**Step 1: Clone the repository**
Clone this repository to your local machine or server instance.

```bash
git clone https://github.com/kiuyha/dwh-socio-economic-etl
``` 

**Step 2: Prepare the environment**
Navigate into the project directory and install the required dependencies.
```bash
cd dwh-socio-economic-etl
```

**Step 3: Database and API configuration**
Configure your connection strings for Supabase and `.env` file.

**Step 4: Deploy the Airflow DAG**
A deployment script is provided to pull the latest changes and move the DAG file directly to your Airflow directory. Make the script executable and run it, passing your local Airflow `dags` folder path as an argument.
```bash
chmod +x deploy.sh
./deploy.sh
```

**Step 5: Execute pipeline**
Verify your Apache Airflow scheduler and webserver are active. Airflow will pick up the `socio_economic_etl_dag.py` file and manage the daily extraction and loading tasks automatically.