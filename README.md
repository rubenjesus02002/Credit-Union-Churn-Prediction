Credit Union Member Churn Analysis
Exploratory Data Analysis & Churn Prediction | Databricks + SQL + Python

Project Overview
I took 10,000 credit union members, analyzed how their personas affected their rate of churn, and built a scalable pipeline in Databricks - the same platform the organization will likely transition to. This is a fun data science approach to a project that will allow me to showcase my SQL/Python skills for both engineering and analysis. 

Business Context
Imagine a system which can predict the rate in which a customer will stop using your product or service based on behavioral analysis - well thats what I built for banking. What does churn mean for a credit union? How fast and how likely our members will stop banking with us. This prediction is valuable because it helps us target members who are more likely churn, to help maintain or strengthen primacy - all while ensuring that our more loyal members are more than satisfied.  

Dataset
Detail               Value
Total Members        10,000
Total Transactions   7.2M
Database Size        ~150MB
Source               Synthetic — modeled on real credit union member behavior patterns

Member Personas
This project uses 7 distinct member personas based on real-world HVCU member behavior patterns.
Embedded Characteristics:

Balances
Transaction Counts
Product Counts
Product Type
Account Usage
Time Series Data:

Length of time between transactions
Length of time between product usage
Persona Types:
Primary Banker: Direct deposit, high transaction count, multiple products - low churn risk (Primacy)
Rate Shopper: Large balance in CD, minimal transactions - churns at CD maturity
Loan-Only: Just auto loan, pays off, then primary savings goes dormant - high churn risk post-payoff
Slow Adopter: Opens account, takes 6 months to activate, eventually becomes engaged - risk early, stable later
Emergency User: Uses PALs frequently, volatile balance, excessive privilege pay - moderate churn risk, needs intervention (Cross Sell)
Seasonal Worker: Works seasonally, inconsistent income/activity, moderate churn
Digital-First: Heavy mobile/online usage, minimal branch visits, tech-savvy


Tech Stack

- Database: SQLite (local) → Data (cloud)
- Language: Python, SQL (Spark SQL)
- Libraries: Pandas, Matplotlib, Seaborn
- Platform: Data (Delta Lake)
- Version Control: GitHub


Project Structure
├── 01_data_generation/       # Scripts to generate synthetic dataset
├── 02_exploratory_analysis/  # SQLite, pandas, Jupyter. The "before" story.
├── 03_data_notebook/   # Spark SQL, Delta Tables, Medallion. The "after" story
├── working_folder/           # Scratch work and experiments
└── README.md

Key Findings
- Loan-Only members had the highest churn rate at 43.8%
- Primary Bankers had the lowest at 3.35%
- Active members maintained ~$2,000 higher average balance than churned members


Analysis Walkthrough
Phase 1 — Data Familiarization
...
Phase 2 — Churn Breakdown
...
Phase 3 — Balance & Transaction Analysis
...
Phase 4 — Persona Deep Dive
...
Phase 5 — Data Migration
...

Data Highlights
Data offered the ability to transition this project to a scalable cloud based platform that allowed for seamless BI Tool integration. 

Delta Tables — ACID transactions ensure data integrity for sensitive financial data
Time Travel — Ability to audit and query member data at any point in history
Spark SQL — Scalable queries across millions of transactions
Catalog Structure — Organized data governance across schemas


How To Run Locally
bash# Step 1 — Generate the database
cd 01_data_generation
python random_data_generation.py
mv credit_union_data.db ../data/

# Step 2 — Run the analysis
cd 02_exploratory_analysis
jupyter notebook

Author
Ruben Jesus Rodriguez
Aspiring Business Analyst | Credit Union Domain | Databricks 
GitHub | LinkedIn

This project was built as part of interview preparation for a Business Analyst role focused on a Databricks migration in a credit union environment.
