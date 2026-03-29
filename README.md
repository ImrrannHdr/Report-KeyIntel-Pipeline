# 🔍 CyberTipline Intel Pipeline
### Serverless Document Intelligence & IP Enrichment Platform on AWS

![Architecture](https://img.shields.io/badge/Cloud-AWS-FF9900?logo=amazonaws&logoColor=white)
![Bedrock](https://img.shields.io/badge/AI-Amazon%20Bedrock-6C3483?logo=amazonaws)
![Glue](https://img.shields.io/badge/ETL-AWS%20Glue%20%2B%20PySpark-E97627?logo=apachespark&logoColor=white)
![Lambda](https://img.shields.io/badge/Serverless-AWS%20Lambda-FF9900?logo=awslambda&logoColor=white)
![EventBridge](https://img.shields.io/badge/Orchestration-EventBridge-E97627)
![Status](https://img.shields.io/badge/Status-Production-brightgreen)

---

## Overview

An **end-to-end serverless data pipeline** that leverages **Amazon Bedrock Data Automation (BDA)** to extract structured intelligence from unstructured CyberTipline PDF reports, enriches extracted IP addresses via third-party geolocation APIs, and routes transformed outputs to operator-segmented S3 destinations for downstream law enforcement analytics.

Built entirely on **event-driven, cloud-native AWS services** — zero servers, zero manual intervention.

---

## Architecture

```
CyberTipline PDF Report
        │
        ▼
┌─────────────────────┐
│  Amazon Bedrock DA  │  ◄── Custom Blueprint (JSON schema extraction)
│  Document Processor │
└─────────┬───────────┘
          │  result.json → S3
          ▼
┌─────────────────────┐
│  Amazon EventBridge │  ◄── Wildcard rule: */custom_output/*/result.json
│  Event Rule         │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   AWS Lambda        │  ◄── Event router + Glue job trigger
│   (Orchestrator)    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   AWS Glue (Job 1)  │  ◄── PySpark: IP extraction, timestamp normalisation
│   IP Enrichment     │       IPInfo.io geolocation API enrichment
└─────────┬───────────┘
          │  _enriched.csv → S3
          ▼
┌─────────────────────┐
│   AWS Lambda        │  ◄── S3 event trigger on _enriched.csv
│   (Orchestrator 2)  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   AWS Glue (Job 2)  │  ◄── PySpark: Mobile normalisation, operator
│   Transform & Route │       classification, PTCL IP filtering
└─────────┬───────────┘
          │
          ▼
┌─────────────────────────────────────────────┐
│              Output S3 Bucket               │
│  mobilink/  ufone/  telenor/  zong/  ptcl/  │
└─────────────────────────────────────────────┘
```

---

## Key Technical Highlights

### AI-Powered Document Intelligence
- Configured a **custom Amazon Bedrock Data Automation blueprint** to extract structured fields (suspect IPs, IMEI, email, mobile, priority level) from multi-page unstructured PDF documents
- Achieved **>83% confidence** on key field extractions using BDA's explainability scoring

### Event-Driven Orchestration
- Designed an **EventBridge wildcard pattern rule** (`*/custom_output/*/result.json`) to selectively route S3 object creation events, filtering out `standard_output` noise without any application-level logic overhead
- Eliminated polling with a fully **push-based, asynchronous trigger chain**: S3 → EventBridge → Lambda → Glue

### Scalable ETL with PySpark on AWS Glue
- Built two-stage PySpark ETL jobs on **AWS Glue Serverless** handling:
  - **Stage 1**: Regex-based IP and timestamp extraction, UTC→PKT timezone conversion, IPInfo.io API enrichment with in-memory caching to minimise API calls
  - **Stage 2**: Pakistani telecom operator classification (Mobilink, Ufone, Telenor, Zong) via prefix-based routing, PTCL IP filtering, deduplication, and partitioned CSV output

### Data Enrichment
- Integrated **IPInfo.io REST API** for real-time IP geolocation enrichment (ASN, organisation, country, region, city)
- Implemented **IP-level caching** within Glue job execution to deduplicate API calls across repeated IPs in the same report

---

## Tech Stack

| Layer | Technology |
|---|---|
| Document AI | Amazon Bedrock Data Automation (BDA) |
| Event Routing | Amazon EventBridge (wildcard rules) |
| Orchestration | AWS Lambda (Python 3.14) |
| ETL Engine | AWS Glue + Apache PySpark |
| Storage | Amazon S3 (multi-prefix partitioned output) |
| IP Enrichment | IPInfo.io Geolocation API |
| Language | Python 3 (boto3, pandas, pyspark, requests) |
| IaC | Manual (AWS Console) — Terraform migration planned |

---

## Repository Structure

```
cybertipline-intel-pipeline/
│
├── glue_jobs/
│   ├── job1_ip_enrichment.py         # Stage 1: BDA output → enriched CSV
│   └── job2_transform_route.py       # Stage 2: Mobile classification + PTCL filter
│
├── lambda_functions/
│   ├── trigger_job1/
│   │   └── lambda_function.py        # EventBridge → Glue Job 1 trigger
│   └── trigger_job2/
│       └── lambda_function.py        # S3 enriched CSV → Glue Job 2 trigger
│
├── eventbridge/
│   └── custom_output_rule.json       # EventBridge pattern for custom_output filter
│
├── bedrock/
│   └── blueprint_schema.json         # BDA blueprint for CyberTipline field extraction
│
├── sample_data/
│   └── sample_enriched.csv           # Anonymised sample output
│
└── README.md
```

---

## Pipeline Execution Flow

1. A CyberTipline PDF report is submitted to **Amazon Bedrock Data Automation**
2. BDA processes the document against the custom blueprint and writes `result.json` to S3 under `bedrock-output/{report_id}/{job_id}/custom_output/{index}/result.json`
3. **EventBridge** matches the S3 object creation event using a wildcard key pattern and invokes **Lambda (Orchestrator 1)**
4. Lambda extracts the bucket name and object key from the EventBridge `detail` payload and triggers **Glue Job 1** with those parameters
5. Glue Job 1 reads the `result.json`, parses IP entries using regex, converts timestamps from UTC to PKT (+5:00), calls the **IPInfo.io API** for each unique IP, and writes `{report_id}_enriched.csv` to `enriched/{report_id}/`
6. A second S3 event on the enriched CSV triggers **Lambda (Orchestrator 2)**, which starts **Glue Job 2**
7. Glue Job 2 normalises Pakistani mobile numbers, classifies them by telecom operator, filters PTCL-hosted IPs, deduplicates records, and writes partitioned CSV outputs to operator-specific S3 prefixes

---

## Skills Demonstrated

- Serverless & event-driven architecture design on AWS
- AI/ML document processing with Amazon Bedrock
- PySpark ETL development on AWS Glue
- REST API integration inside distributed compute jobs
- S3 event routing with EventBridge wildcard patterns
- Multi-stage pipeline orchestration with AWS Lambda
- Regex-based data extraction and normalisation
- Geospatial IP enrichment and telecom classification

---

## Setup & Deployment

### Prerequisites
- AWS account with permissions for S3, Lambda, Glue, EventBridge, Bedrock
- IPInfo.io API token
- Python 3.8+

### Steps

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/cybertipline-intel-pipeline.git
cd cybertipline-intel-pipeline
```

2. **Deploy Glue jobs** — upload scripts from `glue_jobs/` to your Glue script S3 bucket and create jobs pointing to them

3. **Deploy Lambda functions** — zip and deploy each function in `lambda_functions/`, setting the `GLUE_JOB_NAME` and `IPINFO_TOKEN` environment variables

4. **Create EventBridge rule** — import the pattern from `eventbridge/custom_output_rule.json`, set target as Lambda (Orchestrator 1)

5. **Enable EventBridge on S3 bucket** — Bucket → Properties → Amazon EventBridge → On

6. **Create S3 event notification** on the enriched output bucket — suffix `_enriched.csv` → Lambda (Orchestrator 2)

---

## Author

**[Your Name]**
Data Engineer | AWS | PySpark | Serverless Pipelines

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin)](https://linkedin.com/in/YOUR_PROFILE)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?logo=github)](https://github.com/YOUR_USERNAME)

---

> *This project was built as part of real-world law enforcement data infrastructure work. All sample data is fully anonymised.*
