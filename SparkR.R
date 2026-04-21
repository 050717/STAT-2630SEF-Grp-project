install.packages("sparklyr")
install.packages("tidyr")
install.packages("dplyr")

library(sparklyr)
library(tidyr)
library(dplyr)

Sys.setenv(SPARK_HOME = "/usr/local/spark-3.5.7-bin-hadoop3")
Sys.setenv(JAVA_HOME = "/usr/lib/jvm/default-java")

sc <- spark_connect(master = "local", spark_home = "/usr/local/spark-3.5.7-bin-hadoop3")

sdf <- spark_read_csv(sc, name = "benford", path = "/content/benford_summary_stats.csv", header = TRUE, infer_schema = TRUE)

mad_result <- sdf %>% select(label, dimension, mad) %>% collect()

mad_result$label <- as.character(mad_result$label)
mad_result$dimension <- as.character(mad_result$dimension)
mad_result$mad <- as.numeric(mad_result$mad)

make_text <- function(code_points) intToUtf8(code_points)

original_dimension_keys <- c(
  make_text(c(31532,19968,20301,40,21313,36827,5236,41)),
  make_text(c(31532,19968,20301,40,20843,36827,5236,41)),
  make_text(c(31532,19968,20301,40,21313,20845,36827,5236,41)),
  make_text(c(21069,20004,20301,40,21313,36827,5236,41)),
  make_text(c(31532,20108,20301,40,21313,36827,5236,41)),
  make_text(c(31532,19977,20301,40,21313,36827,5236,41)),
  make_text(c(21069,19977,20301,40,21313,36827,5236,41))
)

english_dimension_keys <- c(
  "First Digit (Decimal)",
  "First Digit (Octal)",
  "First Digit (Hexadecimal)",
  "First Two Digits (Decimal)",
  "Second Digit (Decimal)",
  "Third Digit (Decimal)",
  "First Three Digits (Decimal)"
)

mad_result$dimension <- dplyr::recode(mad_result$dimension, !!!stats::setNames(english_dimension_keys, original_dimension_keys))

judge <- function(MAD) {
  MAD <- as.character(MAD)
  if (substr(MAD, 1, 5) == "0.000") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.001") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.002") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.003") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.004") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.005") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.006") {
    g <- "Close conformity (highly natural)"
  } else if (substr(MAD, 1, 5) == "0.007") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.008") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.009") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.010") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.011") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.012") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.013") {
    g <- "Acceptable (natural)"
  } else if (substr(MAD, 1, 5) == "0.014") {
    g <- "Acceptable (natural)"
  } else {
    g <- "Nonconformity (unnatural)"
  }
  return(g)
}

mad_result$madGrade <- sapply(mad_result$mad, judge)

print(mad_result)

mad_wide <- mad_result %>%
  pivot_wider(id_cols = label, names_from = dimension, values_from = madGrade)

print(mad_wide)

finalJudge <- function(row) {
  core <- row$`First Digit (Decimal)`
  sec1 <- row$`First Digit (Octal)`
  sec2 <- row$`First Digit (Hexadecimal)`
  norm1 <- row$`First Two Digits (Decimal)`
  norm2 <- row$`Second Digit (Decimal)`
  norm3 <- row$`Third Digit (Decimal)`
  norm4 <- row$`First Three Digits (Decimal)`

  allGrades <- c(core, sec1, sec2, norm1, norm2, norm3, norm4)
  total_N <- sum(allGrades == "Nonconformity (unnatural)")
  sec_N <- sum(c(sec1, sec2) == "Nonconformity (unnatural)")
  normal_N <- sum(c(norm1, norm2, norm3, norm4) == "Nonconformity (unnatural)")

  if (core == "Close conformity (highly natural)" &&
      sec1 == "Close conformity (highly natural)" && sec2 == "Close conformity (highly natural)" &&
      norm1 == "Close conformity (highly natural)" && norm2 == "Close conformity (highly natural)" &&
      norm3 == "Close conformity (highly natural)" && norm4 == "Close conformity (highly natural)") {
    return("Close conformity (highly natural)")
  } else if (core == "Nonconformity (unnatural)" && sec_N == 2) {
    return("Severe nonconformity (severely unnatural)")
  } else if (core != "Nonconformity (unnatural)" && sec_N < 2 && normal_N < 3) {
    return("Acceptable (natural)")
  } else {
    return("Nonconformity (unnatural)")
  }
}

result <- data.frame(label = mad_wide$label, final_grade = "")

for (i in 1:nrow(mad_wide)) {
  result$final_grade[i] <- finalJudge(mad_wide[i, ])
}

print(result)

result_sdf <- copy_to(sc, result, name = "result", overwrite = TRUE)

spark_write_csv(result_sdf, path = "/content/benford_result", mode = "overwrite", header = TRUE)

spark_disconnect(sc)
