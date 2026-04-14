# Credit Union Member Churn Analysis
### Exploratory Data Analysis & Churn Prediction | Databricks + SQL + Python

---

## Project Overview
I took 10,000 credit union members, analyzed how their personas affected their rate of churn, and built a scalable pipeline in Databricks - the same platform the organization will likely transition to. This is a fun data science approach to a project that will allow me to showcase my SQL/Python skills for both engineering and analysis.

---

## Business Context
Imagine a system which can predict the rate in which a customer will stop using your product or service based on behavioral analysis - well that's what I built for banking. What does churn mean for a credit union? It measures how fast and how likely our members will stop banking with us. This prediction is valuable because it helps us target members who are more likely to churn, to help maintain or strengthen primacy - all while ensuring that our more loyal members are more than satisfied.

---

## Dataset
| Detail | Value |
|--------|-------|
| Total Members | 10,000 |
| Total Transactions | 7.2M |
| Database Size | ~150MB |
| Source | Synthetic — modeled on real credit union member behavior patterns |

### Member Personas
This project uses 7 distinct member personas based on real-world HVCU member behavior patterns.

**Embedded Characteristics:**
- Balances
- Transaction Counts
- Product Counts
- Product Type
- Account Usage

**Time Series Data:**
- Length of time between transactions
- Length of time between product usage

**Persona Types:**
| Persona | Description | Churn Risk |
|---------|-------------|------------|
| Primary Banker | Direct deposit, high transaction count, multiple products | Low |
| Rate Shopper | Large balance in CD, minimal transactions — churns at CD maturity | Medium |
| Loan-Only | Auto loan only, goes dormant post-payoff | High |
| Slow Adopter | Takes 6 months to activate, eventually becomes engaged | Medium |
| Emergency User | Uses PALs frequently, volatile balance, needs Cross Sell intervention | Medium |
| Seasonal Worker | Inconsistent income/activity, moderate churn | Medium |
| Digital-First | Heavy mobile/online usage, minimal branch visits, tech-savvy | Low |

---

## Tech Stack
- **Database:** SQLite (local) → Databricks (cloud)
- **Language:** Python, SQL (Spark SQL)
- **Libraries:** Pandas, Matplotlib, Seaborn
- **Platform:** Databricks (Delta Lake)
- **Architecture:** Medallion (Bronze → Silver → Gold)
- **Version Control:** GitHub

---

## Project Structure
```
├── 01_data_generation/       # Scripts to generate synthetic dataset
├── 02_exploratory_analysis/  # SQLite, pandas, Jupyter. The "before" story.
├── 03_databricks_notebook/   # Spark SQL, Delta Tables, Medallion. The "after" story.
├── working_folder/           # Scratch work and experiments
└── README.md
```

---

## Dashboard & Key Findings

![Member Churn Dashboard](Member Churn Dashboard.png)

- **Loan-Only members** had the highest churn rate at **43.8%** — primarily post loan payoff
- **Primary Bankers** had the lowest churn at **3.35%** with 648 avg transactions per member
- **Active members** maintained ~$2,000 higher average balance than churned members

---

## Analysis Walkthrough

### Phase 1 — Data Familiarization
...

### Phase 2 — Churn Breakdown
...

### Phase 3 — Balance & Transaction Analysis
...

### Phase 4 — Persona Deep Dive
...

### Phase 5 — Databricks Migration
...

---

## Databricks Highlights
Databricks offered the ability to transition this project to a scalable cloud based platform that allowed for seamless BI Tool integration.

| Layer | Schema | Purpose |
|-------|--------|---------|
| 🟤 Bronze | `workspace.bronze` | Raw ingested data — untouched |
| ⚪ Silver | `workspace.silver` | Cleaned, deduplicated, typed |
| 🟡 Gold | `workspace.gold` | BI-ready aggregations and insights |

- **Delta Tables** — ACID transactions ensure data integrity for sensitive financial data
- **Time Travel** — Ability to audit and query member data at any point in history
- **Spark SQL** — Scalable queries across millions of transactions
- **Catalog Structure** — Organized data governance across schemas

---

## How To Run Locally

```bash
# Step 1 — Generate the database
cd 01_data_generation
python random_data_generation.py
mv credit_union_data.db ../data/

# Step 2 — Run the analysis
cd 02_exploratory_analysis
jupyter notebook
```

---

## Author
**Ruben Jesus Rodriguez**
*Aspiring Business Analyst | Credit Union Domain | Databricks Enthusiast*

[GitHub](https://github.com/rubenjesus02002) | [LinkedIn](#)

---
*This project was built as part of interview preparation for a Business Analyst role focused on a Databricks migration in a credit union environment.*
