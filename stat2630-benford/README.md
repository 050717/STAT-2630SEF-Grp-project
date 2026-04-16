# STAT2630SEF Group Project — Benford's Law Analysis

> Bank fraud transaction dataset analysis using Benford's Law  
> Pipeline: Kaggle CSV → ETL (R) → MongoDB Atlas → Spark (Colab) → Power BI

## Team Members

| Name | Student ID | Responsibility |
|------|-----------|----------------|
| 于建超 | XXXXXXXX | R Analysis, MongoDB, GitHub |
| 徐睿涵 | XXXXXXXX | Power BI, Visualization |
| （其他组员） | XXXXXXXX | Spark, Report Writing |

## Project Pipeline

```
Kaggle CSV
    ↓
R (ETL + Benford Analysis)
    ↓
MongoDB Atlas (Data Warehouse)
    ↓
Spark / Google Colab (Large-scale Processing)
    ↓
Power BI (Dashboard Visualization)
```

## Repository Structure

```
stat2630-benford/
├── data/
│   └── .gitkeep              # Raw CSV not committed (too large)
├── r/
│   ├── benford_main.R        # Main analysis script (ETL + Benford + MongoDB)
│   └── benford_multi.R       # Multi-column batch analysis
├── spark/
│   └── benford_spark.ipynb   # Google Colab Spark notebook
├── powerbi/
│   └── benford_dashboard.pbix
├── report/
│   └── STAT2630_Group_Report.docx
├── .gitignore
└── README.md
```

## How to Run

### R Analysis (Local)
```r
# 1. Install dependencies
install.packages(c("readr","dplyr","ggplot2","scales","gridExtra","tidyr","mongolite"))

# 2. Set your MongoDB Atlas connection string in benford_main.R
#    MONGO_URL <- "mongodb+srv://<user>:<password>@<cluster>.mongodb.net/"

# 3. Run
source("r/benford_main.R")
```

### Spark Analysis (Google Colab)
Open `spark/benford_spark.ipynb` in Google Colab and run all cells.  
MongoDB Atlas connection string required.

## Dataset
- Source: Kaggle — Bank Transactions Fraud Detection
- Rows: ~1,000,000
- Key columns: `TransactionAmount`, `AccountBalance`, `IsFraud`
- **Not included in repo** (size limit). Download from Kaggle and place in `data/`.

## Key Findings
- `TransactionAmount` shows **partial conformity** to Benford's Law
- Digit 1 and 9 show statistically significant deviation (Z-test)
- MAD score indicates **acceptable conformity** range
- Fraudulent vs. non-fraudulent transactions show distinct digit distribution patterns
