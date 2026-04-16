# ============================================================
#  STAT2630SEF — Benford's Law with PySpark
#  Google Colab Notebook (save as benford_spark.ipynb)
#  Pipeline: MongoDB Atlas → Spark → Analysis → Write back
# ============================================================
#
#  Cell 1: Install dependencies
# ============================================================

# !pip install pyspark pymongo dnspython -q

# ============================================================
#  Cell 2: Imports & Config
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

MONGO_URL = input("MongoDB Atlas connection string: ")
MONGO_DB  = "stat2630"

# ============================================================
#  Cell 3: Start Spark
# ============================================================

spark = SparkSession.builder \
    .appName("STAT2630_Benford") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark version:", spark.version)

# ============================================================
#  Cell 4: Load from MongoDB Atlas into Spark
# ============================================================

client = MongoClient(MONGO_URL)
db     = client[MONGO_DB]

# Read the clean transactions written by R
docs = list(db["transactions_clean"].find({}, {"_id": 0,
    "TransactionAmount": 1, "AccountBalance": 1, "IsFraud": 1}))

pdf = pd.DataFrame(docs)
print(f"Loaded {len(pdf):,} rows from MongoDB Atlas")

# Convert to Spark DataFrame
sdf = spark.createDataFrame(pdf)
sdf.printSchema()
sdf.show(5)

# ============================================================
#  Cell 5: UDF — extract first digit
# ============================================================

@F.udf(returnType=IntegerType())
def first_digit_udf(x):
    if x is None or x <= 0:
        return None
    s = f"{abs(x):.10f}".replace(".", "").lstrip("0")
    return int(s[0]) if s else None

sdf = sdf.withColumn("first_digit_amount",  first_digit_udf(F.col("TransactionAmount"))) \
         .withColumn("first_digit_balance", first_digit_udf(F.col("AccountBalance")))

# ============================================================
#  Cell 6: Benford analysis function (Spark)
# ============================================================

benford_expected = {d: np.log10(1 + 1/d) for d in range(1, 10)}

def spark_benford(sdf, digit_col, filter_col=None, filter_val=None, label=""):
    df = sdf
    if filter_col and filter_val is not None:
        df = df.filter(F.col(filter_col) == filter_val)

    counts = df.filter(F.col(digit_col).between(1, 9)) \
               .groupBy(digit_col) \
               .count() \
               .orderBy(digit_col) \
               .toPandas()

    counts.columns = ["digit", "count"]
    counts = counts.set_index("digit").reindex(range(1, 10), fill_value=0).reset_index()

    n          = counts["count"].sum()
    obs        = counts["count"] / n
    exp        = np.array([benford_expected[d] for d in range(1, 10)])

    # MAD
    mad = np.mean(np.abs(obs - exp))

    # Chi-square
    from scipy.stats import chisq
    chi_stat = np.sum((counts["count"] - n * exp) ** 2 / (n * exp))
    from scipy.stats import chi2
    chi_p = 1 - chi2.cdf(chi_stat, df=8)

    # Cramer's V
    cramers_v = np.sqrt(chi_stat / (n * 8))

    # Z-scores
    z = (obs - exp) / np.sqrt(exp * (1 - exp) / n)

    result = pd.DataFrame({
        "label":    label,
        "digit":    range(1, 10),
        "observed": obs.values,
        "expected": exp,
        "z_score":  z.values,
        "n":        n,
        "mad":      mad,
        "cramers_v": cramers_v,
        "chi_p":    chi_p
    })
    return result

# Run analyses
r_all    = spark_benford(sdf, "first_digit_amount",  label="All Transactions")
r_fraud  = spark_benford(sdf, "first_digit_amount",  "IsFraud", 1, "Fraudulent")
r_legit  = spark_benford(sdf, "first_digit_amount",  "IsFraud", 0, "Legitimate")
r_bal    = spark_benford(sdf, "first_digit_balance",  label="Account Balance")

combined = pd.concat([r_all, r_fraud, r_legit, r_bal], ignore_index=True)
print(combined[["label","digit","observed","expected","z_score"]].head(20))

# ============================================================
#  Cell 7: Write Spark results back to MongoDB
# ============================================================

col_spark = db["benford_spark_results"]
col_spark.drop()
col_spark.insert_many(combined.to_dict("records"))
print(f"✓ Wrote {col_spark.count_documents({})} docs to benford_spark_results")

# Summary
summary = combined.groupby("label").agg(
    n=("n","first"), mad=("mad","first"),
    cramers_v=("cramers_v","first"), chi_p=("chi_p","first")
).reset_index()

col_spark_summary = db["benford_spark_summary"]
col_spark_summary.drop()
col_spark_summary.insert_many(summary.to_dict("records"))
print(summary)

# ============================================================
#  Cell 8: Plot
# ============================================================

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for ax, (label, grp) in zip(axes, combined.groupby("label")):
    ax.bar(grp["digit"], grp["observed"], color="#4472C4", alpha=0.8, label="Observed")
    ax.plot(grp["digit"], grp["expected"], "ro-", label="Benford Expected")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    mad_val = grp["mad"].iloc[0]
    ax.set_title(f"{label}\nMAD={mad_val:.4f}")
    ax.set_xlabel("First Digit")
    ax.legend()

plt.tight_layout()
plt.savefig("benford_spark_results.png", dpi=150)
plt.show()
print("Plot saved as benford_spark_results.png")

spark.stop()
