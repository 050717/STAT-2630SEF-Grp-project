# ============================================================
#  STAT2630SEF Group Project — Benford's Law Analysis
#  Pipeline: CSV → ETL → MongoDB Atlas → Benford Analysis
#  plotdata collections are Power BI ready
# ============================================================

library(readr); library(dplyr); library(ggplot2)
library(scales); library(gridExtra); library(tidyr); library(mongolite)

# ============================================================
#  CONFIG — 改这里
# ============================================================
MONGO_URL  <- "mongodb+srv://<user>:<password>@<cluster>.mongodb.net/"
MONGO_DB   <- "stat2630"
DATA_PATH  <- "../data/bank_transactions_data_2.csv"

# ============================================================
#  1. LOAD & ETL
# ============================================================
cat("[ 1/5 ] Loading data...\n")
raw <- read_csv(DATA_PATH, show_col_types = FALSE)

dataClean <- raw %>%
  filter(TransactionAmount > 0, !is.na(TransactionAmount)) %>%
  filter(AccountBalance  > 0, !is.na(AccountBalance))  %>%
  distinct()

cat(sprintf("       Loaded %d rows → cleaned %d rows\n", nrow(raw), nrow(dataClean)))

# ============================================================
#  2. WRITE CLEAN DATA TO MONGODB
# ============================================================
cat("[ 2/5 ] Writing clean data to MongoDB Atlas...\n")
tryCatch({
  col_raw <- mongo(collection = "transactions_clean", db = MONGO_DB, url = MONGO_URL)
  col_raw$drop()
  col_raw$insert(dataClean)
  cat(sprintf("       ✓ Inserted %d docs into transactions_clean\n", col_raw$count()))
}, error = function(e) cat("       ✗ MongoDB write failed:", conditionMessage(e), "\n"))

# ============================================================
#  3. BENFORD ANALYSIS FUNCTION
# ============================================================
cat("[ 3/5 ] Running Benford analysis...\n")

benford_expected <- log10(1 + 1/1:9)

extract_first_digit <- function(x) {
  x <- abs(x[x > 0])
  as.integer(substr(gsub("0\\.", "", formatC(x, format = "fg", digits = 15)), 1, 1))
}

run_benford <- function(values, label) {
  digits <- extract_first_digit(values)
  digits <- digits[digits %in% 1:9]
  n      <- length(digits)

  obs_count <- tabulate(digits, nbins = 9)
  obs_prop  <- obs_count / n
  exp_prop  <- benford_expected

  # Chi-square
  chi_result <- chisq.test(obs_count, p = exp_prop)

  # MAD
  mad_val <- mean(abs(obs_prop - exp_prop))

  # Cramer's V
  chi_stat <- chi_result$statistic
  cramers_v <- sqrt(chi_stat / (n * (9 - 1)))

  # Z-test per digit
  z_scores <- (obs_prop - exp_prop) / sqrt(exp_prop * (1 - exp_prop) / n)

  # plotdata — this is what Power BI will consume
  plotdata <- data.frame(
    label    = label,
    digit    = 1:9,
    observed = obs_prop,
    expected = exp_prop,
    z_score  = z_scores,
    stringsAsFactors = FALSE
  )

  list(
    label     = label,
    n         = n,
    plotdata  = plotdata,
    chi_p     = chi_result$p.value,
    mad       = mad_val,
    cramers_v = as.numeric(cramers_v)
  )
}

# Run for TransactionAmount (all, fraud, non-fraud)
result_all   <- run_benford(dataClean$TransactionAmount,                          "All Transactions")
result_fraud <- run_benford(dataClean$TransactionAmount[dataClean$IsFraud == 1],  "Fraudulent")
result_legit <- run_benford(dataClean$TransactionAmount[dataClean$IsFraud == 0],  "Legitimate")

# Also run AccountBalance
result_bal   <- run_benford(dataClean$AccountBalance, "Account Balance")

all_results <- list(result_all, result_fraud, result_legit, result_bal)

# ============================================================
#  4. WRITE PLOTDATA TO MONGODB (Power BI source)
# ============================================================
cat("[ 4/5 ] Writing plotdata to MongoDB (Power BI ready)...\n")
tryCatch({
  col_plot <- mongo(collection = "benford_plotdata", db = MONGO_DB, url = MONGO_URL)
  col_plot$drop()

  combined_plotdata <- bind_rows(lapply(all_results, `[[`, "plotdata"))
  col_plot$insert(combined_plotdata)
  cat(sprintf("       ✓ Inserted %d docs into benford_plotdata\n", col_plot$count()))

  # Summary stats collection
  col_stats <- mongo(collection = "benford_summary", db = MONGO_DB, url = MONGO_URL)
  col_stats$drop()
  summary_df <- data.frame(
    label     = sapply(all_results, `[[`, "label"),
    n         = sapply(all_results, `[[`, "n"),
    chi_p     = sapply(all_results, `[[`, "chi_p"),
    mad       = sapply(all_results, `[[`, "mad"),
    cramers_v = sapply(all_results, `[[`, "cramers_v"),
    stringsAsFactors = FALSE
  )
  col_stats$insert(summary_df)
  cat(sprintf("       ✓ Inserted %d docs into benford_summary\n", col_stats$count()))

}, error = function(e) cat("       ✗ MongoDB plotdata write failed:", conditionMessage(e), "\n"))

# ============================================================
#  5. LOCAL PLOTS (ggplot2)
# ============================================================
cat("[ 5/5 ] Generating plots...\n")

plot_benford <- function(res) {
  pd <- res$plotdata
  ggplot(pd, aes(x = factor(digit))) +
    geom_col(aes(y = observed), fill = "#4472C4", alpha = 0.8) +
    geom_line(aes(y = expected, group = 1), color = "red", linewidth = 1) +
    geom_point(aes(y = expected), color = "red", size = 2) +
    scale_y_continuous(labels = percent_format()) +
    labs(
      title    = paste("Benford's Law —", res$label),
      subtitle = sprintf("n = %s | MAD = %.4f | Cramér's V = %.4f | p = %s",
                         format(res$n, big.mark = ","),
                         res$mad, res$cramers_v,
                         ifelse(res$chi_p < 1e-4,
                                formatC(res$chi_p, format = "e", digits = 3),
                                round(res$chi_p, 4))),
      x = "First Digit", y = "Proportion"
    ) +
    theme_minimal(base_size = 12)
}

plots <- lapply(all_results, plot_benford)
grid.arrange(grobs = plots, ncol = 2)

cat("\n=== SUMMARY ===\n")
print(summary_df)
cat("\nDone. Collections in MongoDB Atlas:\n")
cat("  - transactions_clean  (raw ETL output)\n")
cat("  - benford_plotdata    (Power BI source)\n")
cat("  - benford_summary     (KPIs)\n")
