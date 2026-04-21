# ============================================================
#  股票数据本福特定律分析脚本
#  数据来源：Yahoo Finance（quantmod，无需API Key）
#  股票：AAPL, GOOGL, MSFT, META, INTC, AMD
#  分析列：收盘价（Close）、交易量（Volume）
# ============================================================

#install.packages(c("quantmod","mongolite","dplyr",
#                 "ggplot2","scales","gridExtra"))

library(quantmod)
library(mongolite)
library(dplyr)
library(ggplot2)
library(scales)
library(gridExtra)

# ============================================================
#  配置
# ============================================================

STOCKS <- c("AAPL", "GOOGL", "MSFT", "META", "INTC", "AMD")

MONGO_URL        <- readline("请输入 MongoDB Atlas 连接字符串: ")
MONGO_DB         <- "stat2630"
MONGO_COLLECTION <- "stock_benford"

# ============================================================
#  从 Yahoo Finance 获取股票数据
# ============================================================

cat("正在从 Yahoo Finance 获取股票数据...\n")

fetch_yahoo <- function(symbol) {
  tryCatch({
    getSymbols(symbol, src="yahoo", from="2000-01-01",
               auto.assign=TRUE, warnings=FALSE)
    xts_obj <- get(symbol)
    df <- data.frame(
      date   = as.Date(index(xts_obj)),
      open   = as.numeric(Op(xts_obj)),
      high   = as.numeric(Hi(xts_obj)),
      low    = as.numeric(Lo(xts_obj)),
      close  = as.numeric(Cl(xts_obj)),
      volume = as.numeric(Vo(xts_obj)),
      symbol = symbol,
      stringsAsFactors = FALSE
    )
    df <- df[df$close > 0 & !is.na(df$close), ]
    rownames(df) <- NULL
    cat(sprintf("  ✓ %s 获取成功（%d 行）\n", symbol, nrow(df)))
    return(df)
  }, error = function(e) {
    cat(sprintf("  ✗ %s 获取失败：%s\n", symbol, e$message))
    return(NULL)
  })
}

stock_list <- lapply(STOCKS, fetch_yahoo)
all_stocks <- do.call(rbind, stock_list[!sapply(stock_list, is.null)])
rownames(all_stocks) <- NULL

cat(sprintf("\n全部股票数据获取完成，共 %d 行\n", nrow(all_stocks)))

# ============================================================
#  写入 MongoDB Atlas（原始数据）
# ============================================================

cat("\n正在写入 MongoDB Atlas...\n")
tryCatch({
  con_raw <- mongo(
    collection = paste0(MONGO_COLLECTION, "_raw"),
    db         = MONGO_DB,
    url        = MONGO_URL
  )
  con_raw$drop()
  con_raw$insert(all_stocks)
  cat(sprintf("✓ 原始数据已写入集合 '%s_raw'（%d 行）\n",
              MONGO_COLLECTION, nrow(all_stocks)))
}, error = function(e) {
  cat(sprintf("✗ MongoDB 写入失败：%s\n", e$message))
})

# ============================================================
#  本福特分析工具函数
# ============================================================

clean_vec <- function(x) {
  x <- as.character(x)
  x <- gsub("\\.", "", x)
  x <- gsub("^0+", "", x)
  x
}

Benford_1   <- log10(1 + 1/1:9)
Benford_12  <- log10(1 + 1/10:99)
Benford_123 <- log10(1 + 1/100:999)
Benford_2   <- sapply(0:9, function(d) sum(log10(1 + 1/(10*1:9 + d))))
Benford_3   <- sapply(0:9, function(d) sum(sapply(0:9, function(m) sum(log10(1 + 1/(100*1:9 + 10*m + d))))))
Benford_o1  <- log(1 + 1/1:7,  base=8)
Benford_h1  <- log(1 + 1/1:15, base=16)
hex_digits_all <- c(as.character(1:9), letters[1:6])

run_chisq <- function(obs, exp_p, label) {
  valid <- exp_p > 0
  obs   <- obs[valid]; exp_p <- exp_p[valid]
  n     <- sum(obs)
  chi   <- sum((obs - n*exp_p)^2 / (n*exp_p))
  df    <- length(obs) - 1
  p_val <- pchisq(chi, df, lower.tail=FALSE)
  p_str <- if (p_val < 1e-4) sprintf("%e", p_val) else sprintf("%.6f", p_val)
  cat(sprintf("  %-28s chi2=%-10.2f p=%s\n", label, chi, p_str))
  invisible(list(chi_stat=chi, df=df, p_value=p_val, n=n, k=length(obs)))
}

calc_mad <- function(obs, exp_p) mean(abs(obs/sum(obs) - exp_p))
fmt_p    <- function(p) if (p < 1e-4) sprintf("%e", p) else sprintf("%.4f", p)

make_digit_counts <- function(digit_vec, keys, count_name) {
  ref <- setNames(data.frame(keys), "key")
  obs <- data.frame(key=digit_vec[!is.na(digit_vec) & digit_vec != ""])
  obs <- obs %>% count(key, name="n")
  ref %>% left_join(obs, by="key") %>%
    mutate(n = ifelse(is.na(n), 0L, n)) %>%
    pull(n)
}

# ============================================================
#  逐股票逐列分析
# ============================================================

results_list <- list()
plot_list_dec <- list()  # 十进制图
plot_list_hex <- list()  # 进制图

for (sym in STOCKS) {
  for (col in c("close", "volume")) {

    label  <- sprintf("%s %s", sym, col)
    df_sym <- all_stocks %>% filter(symbol == sym)
    vals   <- df_sym[[col]]
    vals   <- vals[!is.na(vals) & vals > 0]
    tmp    <- clean_vec(vals)

    # 提取各位数字
    d1   <- as.integer(substr(tmp, 1, 1))
    d12  <- ifelse(nchar(tmp)<2, NA_integer_, as.integer(substr(tmp,1,2)))
    d123 <- ifelse(nchar(tmp)<3, NA_integer_, as.integer(substr(tmp,1,3)))
    d2   <- ifelse(nchar(tmp)<2, NA_integer_, as.integer(substr(tmp,2,2)))
    d3   <- ifelse(nchar(tmp)<3, NA_integer_, as.integer(substr(tmp,3,3)))
    do1  <- substr(as.character(as.octmode(as.integer(tmp))), 1, 1)
    dh1  <- substr(as.character(as.hexmode(as.integer(tmp))), 1, 1)

    # 频数
    cnt1   <- make_digit_counts(d1,   1:9,             "n")
    cnt12  <- make_digit_counts(d12,  10:99,            "n")
    cnt123 <- make_digit_counts(d123, 100:999,          "n")
    cnt2   <- make_digit_counts(d2,   0:9,              "n")
    cnt3   <- make_digit_counts(d3,   0:9,              "n")
    cnto1  <- make_digit_counts(do1,  as.character(1:7),       "n")
    cnth1  <- make_digit_counts(dh1,  hex_digits_all,   "n")

    cat(sprintf("\n======== %s ========\n", label))
    r1   <- run_chisq(cnt1,   Benford_1,   "第一位数字(十进制)")
    r12  <- run_chisq(cnt12,  Benford_12,  "前两位数字(十进制)")
    r123 <- run_chisq(cnt123, Benford_123, "前三位数字(十进制)")
    r2   <- run_chisq(cnt2,   Benford_2,   "第二位数字(十进制)")
    r3   <- run_chisq(cnt3,   Benford_3,   "第三位数字(十进制)")
    ro1  <- run_chisq(cnto1,  Benford_o1,  "第一位数字(八进制)")
    rh1  <- run_chisq(cnth1,  Benford_h1,  "第一位数字(十六进制)")

    mad_val <- calc_mad(cnt1, Benford_1)
    cat(sprintf("  MAD(第一位): %.6f", mad_val))
    if      (mad_val < 0.006) cat(" → 符合本福特 ✓\n")
    else if (mad_val < 0.012) cat(" → 轻微偏离\n")
    else if (mad_val < 0.015) cat(" → 边缘偏离\n")
    else                      cat(" → 显著偏离\n")

    # 绘图数据
    mk_pd <- function(cnt, benford) {
      data.frame(x=seq_along(cnt), actual=cnt/sum(cnt), benford=benford)
    }
    pd1   <- mk_pd(cnt1,   Benford_1)
    pd12  <- mk_pd(cnt12,  Benford_12)
    pd123 <- mk_pd(cnt123, Benford_123)
    pd2   <- mk_pd(cnt2,   Benford_2)
    pd3   <- mk_pd(cnt3,   Benford_3)
    pdo1  <- mk_pd(cnto1,  Benford_o1)
    pdh1  <- mk_pd(cnth1,  Benford_h1)

    # X轴标签
    pd1$xlab   <- as.character(1:9)
    pd12$xlab  <- as.character(10:99)
    pd123$xlab <- as.character(100:999)
    pd2$xlab   <- as.character(0:9)
    pd3$xlab   <- as.character(0:9)
    pdo1$xlab  <- as.character(1:7)
    pdh1$xlab  <- hex_digits_all

    mk_plot <- function(pd, xlab, r, mad_v, brk=NULL) {
      p <- ggplot(pd, aes(x=factor(xlab, levels=xlab))) +
        geom_col(aes(y=actual, fill="Actual"), width=0.5) +
        geom_line(aes(y=benford, color="Benford", group=1)) +
        geom_point(aes(y=benford, color="Benford"), size=2) +
        labs(x=xlab[1], y="Frequency", fill=NULL, color=NULL,
             subtitle=sprintf("chi2=%.2f p=%s MAD=%.4f",
                              r$chi_stat, fmt_p(r$p_value), mad_v)) +
        scale_y_continuous(labels=scales::percent_format(accuracy=0.1)) +
        scale_fill_manual(values="#1f77b4") +
        scale_color_manual(values="#ff7f0e") +
        theme_minimal(base_size=9) +
        theme(axis.text.x=element_text(size=7))
      if (!is.null(brk)) p <- p + scale_x_discrete(breaks=brk)
      p
    }

    p1 <- mk_plot(pd1,   pd1$xlab,   r1,   calc_mad(cnt1,  Benford_1))
    p2 <- mk_plot(pd12,  pd12$xlab,  r12,  calc_mad(cnt12, Benford_12),
                  as.character(seq(10,99,10)))
    p3 <- mk_plot(pd123, pd123$xlab, r123, calc_mad(cnt123,Benford_123),
                  as.character(seq(100,999,100)))
    p4 <- mk_plot(pd2,   pd2$xlab,   r2,   calc_mad(cnt2,  Benford_2))
    p5 <- mk_plot(pd3,   pd3$xlab,   r3,   calc_mad(cnt3,  Benford_3))
    p6 <- mk_plot(pdo1,  pdo1$xlab,  ro1,  calc_mad(cnto1, Benford_o1))
    p7 <- mk_plot(pdh1,  pdh1$xlab,  rh1,  calc_mad(cnth1, Benford_h1))

    lay1 <- rbind(c(1,2,4), c(NA,3,5))
    grid.arrange(p1,p2,p3,p4,p5, layout_matrix=lay1,
                 widths=c(1,1,1), heights=c(1,1),
                 top=sprintf("%s — 十进制本福特分析", label))

    lay2 <- rbind(c(1,2,3))
    grid.arrange(p1,p6,p7, layout_matrix=lay2,
                 widths=c(1,1,1), heights=c(1,1),
                 top=sprintf("%s — 进制对比", label))

    results_list[[label]] <- data.frame(
      symbol=sym, column=col,
      chi_1=r1$chi_stat,   p_1=r1$p_value,
      chi_12=r12$chi_stat, p_12=r12$p_value,
      chi_2=r2$chi_stat,   p_2=r2$p_value,
      chi_3=r3$chi_stat,   p_3=r3$p_value,
      mad_1=mad_val,
      run_time=as.character(Sys.time())
    )
  }
}

# ============================================================
#  写入检验结果到 MongoDB
# ============================================================

results_df <- do.call(rbind, results_list)
rownames(results_df) <- NULL

tryCatch({
  con_res <- mongo(
    collection = paste0(MONGO_COLLECTION, "_results"),
    db         = MONGO_DB,
    url        = MONGO_URL
  )
  con_res$drop()
  con_res$insert(results_df)
  cat(sprintf("\n✓ 检验结果已写入 MongoDB（%d 行）\n", nrow(results_df)))

  check <- con_res$find('{}', limit=3)
  cat("✓ 验证读回成功：\n")
  print(check[, c("symbol","column","chi_1","p_1","mad_1")])
}, error = function(e) {
  cat(sprintf("✗ MongoDB 结果写入失败：%s\n", e$message))
})

cat("\npipeline 完成：Yahoo Finance → R清洗 → MongoDB Atlas → 本福特分析 → 结果存Atlas\n")
